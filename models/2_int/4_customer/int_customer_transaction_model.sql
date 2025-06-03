{{ config(
    materialized='table',
    description='Customer master analysis with RFM segmentation and comprehensive customer intelligence'
) }}

WITH base_transactions AS (
    SELECT 
        a.source_no_,
        a.document_no_,
        a.posting_date,
        a.sales_amount__actual_,
        a.offline_order_channel, --store location
        b.web_order_id,
        b.online_order_channel, --website, App, CRM
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
        AND sales_channel IN ('Shop','Online')
),

customer_base_metrics AS (
    SELECT 
        source_no_,
        name,
        raw_phone_no_,
        customer_identity_status,
        
        -- Date metrics
        MIN(posting_date) AS customer_acquisition_date,
        MIN(posting_date) AS first_order_date,
        MAX(posting_date) AS last_order_date,
        
        -- Channel Distribution Lists
        STRING_AGG(DISTINCT offline_order_channel, ' | ') AS stores_used,
        STRING_AGG(DISTINCT online_order_channel, ' | ') AS platforms_used,
        
        -- Order counts
        COUNT(DISTINCT 
            CASE 
                WHEN document_no_ LIKE 'INV%' THEN web_order_id 
                ELSE document_no_ 
            END
        ) AS total_order_count,
        
        COUNT(DISTINCT CASE WHEN document_no_ LIKE 'INV%' THEN web_order_id END) AS online_order_count,
        COUNT(DISTINCT CASE WHEN document_no_ NOT LIKE 'INV%' THEN document_no_ END) AS offline_order_count,
        
        -- Sales values
        SUM(sales_amount__actual_) AS total_sales_value,
        SUM(CASE WHEN document_no_ LIKE 'INV%' THEN sales_amount__actual_ ELSE 0 END) AS online_sales_value,
        SUM(CASE WHEN document_no_ NOT LIKE 'INV%' THEN sales_amount__actual_ ELSE 0 END) AS offline_sales_value,
        
        -- YTD Sales
        SUM(CASE 
            WHEN posting_date >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 1, 1)
            AND posting_date <= CURRENT_DATE()
            THEN sales_amount__actual_ 
            ELSE 0 
        END) AS ytd_sales,
        
        -- MTD Sales
        SUM(CASE 
            WHEN posting_date >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 1)
            AND posting_date <= CURRENT_DATE()
            THEN sales_amount__actual_ 
            ELSE 0 
        END) AS mtd_sales
        
    FROM base_transactions
    GROUP BY 1, 2, 3, 4
),

-- Separate CTE for acquisition analysis
customer_acquisition_analysis AS (
    SELECT 
        source_no_,
        
        -- First order analysis
        MIN(posting_date) AS first_order_date,
        MIN(CASE WHEN document_no_ LIKE 'INV%' THEN posting_date END) AS first_online_order_date,
        MIN(CASE WHEN document_no_ NOT LIKE 'INV%' THEN posting_date END) AS first_offline_order_date,
        
        -- First acquisition channels
        ARRAY_AGG(offline_order_channel ORDER BY posting_date LIMIT 1)[OFFSET(0)] AS first_acquisition_store,
        ARRAY_AGG(online_order_channel ORDER BY posting_date LIMIT 1)[OFFSET(0)] AS first_acquisition_platform
        
    FROM base_transactions
    GROUP BY 1
),

-- Join base metrics with acquisition analysis
customer_enriched_metrics AS (
    SELECT 
        cbm.*,
        caa.first_acquisition_store,
        caa.first_acquisition_platform,
        
        -- Acquisition Channel Analysis
        CASE 
            WHEN caa.first_online_order_date = cbm.first_order_date THEN 'Online'
            WHEN caa.first_offline_order_date = cbm.first_order_date THEN 'Offline'
            ELSE 'Unknown'
        END AS customer_acquisition_channel,
        
        -- NEW: Combined Acquisition Channel Detail
        CASE 
            WHEN caa.first_online_order_date = cbm.first_order_date THEN caa.first_acquisition_platform
            WHEN caa.first_offline_order_date = cbm.first_order_date THEN caa.first_acquisition_store
            ELSE 'Unknown'
        END AS customer_acquisition_channel_detail
                
    FROM customer_base_metrics cbm
    LEFT JOIN customer_acquisition_analysis caa ON cbm.source_no_ = caa.source_no_
),

customer_calculated_metrics AS (
    SELECT *,
        -- Derived metrics
        DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) AS recency_days,
        DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, DAY) AS customer_tenure_days,
        total_order_count AS frequency_orders,
        total_sales_value AS monetary_total_value,
        ROUND(total_sales_value / NULLIF(total_order_count, 0), 2) AS monetary_avg_order_value,

        -- Average Monthly Demand (total sales / months with us)
        ROUND(
            total_sales_value / NULLIF(DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, MONTH), 0), 2
        ) AS avg_monthly_demand,
 
        
        
        -- Months since acquisition (numeric for calculations)
        DATE_DIFF(CURRENT_DATE(), customer_acquisition_date, MONTH) AS months_since_acquisition,
        
        
        -- Customer Type: New vs Repeat
        CASE 
            -- New: Only 1 order AND acquired within last 30 days
            WHEN total_order_count = 1 
                 AND customer_acquisition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 
            THEN 'New'
            
            -- Repeat: More than 1 order in history
            WHEN total_order_count > 1 
            THEN 'Repeat'
            
            -- One Time Customer: Only 1 order BUT acquired more than 30 days ago
            WHEN total_order_count = 1 
                 AND customer_acquisition_date < DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) 
            THEN 'One-Time'
            
            ELSE 'Unknown'
        END AS customer_type,
        
        -- Customer Channel Distribution
        CASE 
            WHEN online_order_count > 0 AND offline_order_count > 0 THEN 'Hybrid'
            WHEN online_order_count > 0 AND offline_order_count = 0 THEN 'Online'
            WHEN online_order_count = 0 AND offline_order_count > 0 THEN 'Shop'
            ELSE 'Unknown'
        END AS customer_channel_distribution,
        
        -- Dynamic Customer Acquisition Age Segmentation
        -- Always goes back to January of current year, then groups by full years
        CASE 
            -- MTD: Current month 
            WHEN customer_acquisition_date >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 1)
            THEN 'MTD'
            
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


        
    FROM customer_enriched_metrics
),

customer_rfm_scores AS (
    SELECT *,
        -- R Score: Recency
        CASE 
            WHEN recency_days <= 30 THEN 5
            WHEN recency_days <= 60 THEN 4
            WHEN recency_days <= 90 THEN 3
            WHEN recency_days <= 180 THEN 2
            ELSE 1
        END AS r_score,
        
        -- F Score: Frequency  
        CASE 
            WHEN frequency_orders >= 10 THEN 5
            WHEN frequency_orders >= 6 THEN 4
            WHEN frequency_orders >= 4 THEN 3
            WHEN frequency_orders >= 2 THEN 2
            ELSE 1
        END AS f_score,
        
        -- M Score: Monetary
        CASE 
            WHEN monetary_total_value >= 2000 THEN 5
            WHEN monetary_total_value >= 1000 THEN 4
            WHEN monetary_total_value >= 500 THEN 3
            WHEN monetary_total_value >= 200 THEN 2
            ELSE 1
        END AS m_score
        
    FROM customer_calculated_metrics
),

customer_segments AS (
    SELECT *,
        -- RFM Segment
        CONCAT(r_score, f_score, m_score) AS rfm_segment,
        
        -- Customer Value Tier based on spending percentiles
        CASE 
            WHEN PERCENT_RANK() OVER (ORDER BY monetary_total_value DESC) <= 0.01 THEN 'Top 1%'
            WHEN PERCENT_RANK() OVER (ORDER BY monetary_total_value DESC) <= 0.20 THEN 'Top 20%'
            WHEN PERCENT_RANK() OVER (ORDER BY monetary_total_value DESC) <= 0.60 THEN 'Middle 30-60%'
            ELSE 'Bottom 40%'
        END AS customer_value_segment,
        
        -- Customer Segment Classification
        CASE 
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
            WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'Loyal Customers'
            WHEN r_score <= 2 AND f_score >= 4 AND m_score >= 4 THEN 'Cant Lose Them'
            WHEN r_score >= 4 AND (f_score >= 2 OR m_score >= 3) THEN 'Potential Loyalists'
            WHEN customer_tenure_days <= 90 OR (r_score >= 4 AND f_score = 1) THEN 'New Customers'
            WHEN r_score <= 2 AND f_score >= 2 AND m_score >= 2 THEN 'At Risk'
            WHEN r_score = 1 AND f_score = 1 AND m_score <= 2 THEN 'Lost'
            ELSE 'Others'
        END AS customer_rfm_segment
        
    FROM customer_rfm_scores
)

SELECT * FROM customer_segments
ORDER BY total_sales_value DESC