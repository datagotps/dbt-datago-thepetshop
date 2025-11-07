{{ config(
    materialized='table',
    tags=['transactions', 'daily', 'omnichannel']
) }}

WITH offline_transactions AS (
    SELECT
        -- Date and Identity
        company_source,
        posting_date AS transaction_date,
        'Posting Date' AS date_type,
        unified_customer_id,
        unified_order_id,
        
        -- Channel Classification
        sales_channel,
        sales_channel_sort,
        sales_channel_detail,
        'Offline' AS channel_group,
        affiliate_order_channel,
        
        -- Transaction Details
        transaction_type,
        item_no_,
        item_name,
        invoiced_quantity,
        
        -- Financial Metrics
        sales_amount__actual_ AS revenue_amount,
        --cost_amount__actual_ AS cost_amount,
        discount_amount,
        
        -- Source System
        'ERP' AS source_system,
       -- CAST(document_no_ AS STRING) AS source_document_id
        
    FROM {{ ref('int_order_lines') }}
    WHERE sales_channel != 'Online'
),

online_transactions AS (
    SELECT 
        -- Date and Identity
       'Petshop' as company_source,
        DATE(ofs_order_datetime) AS transaction_date,
        'Order Date' AS date_type,
        customerid AS unified_customer_id,
        weborderno AS unified_order_id,
        
        -- Channel Classification
        'Online' AS sales_channel,
        1 AS sales_channel_sort,
        ordersource AS sales_channel_detail,
        'Online' AS channel_group,
        '' AS affiliate_order_channel,
        
        -- Transaction Details
        'Sale' AS transaction_type,
        sku AS item_no_,
        item_name,
        quantity AS invoiced_quantity,
        
        -- Financial Metrics
        net_subtotal_exclu_tax AS revenue_amount,
       -- 0 AS cost_amount,
        discount_exclu_tax AS discount_amount,
        
        -- Source System
        'OFS' AS source_system,
        --CAST(itemid AS STRING) AS source_document_id
        
    FROM {{ ref('int_occ_order_items') }}
),

combined_transactions AS (
    SELECT * FROM offline_transactions
    UNION ALL
    SELECT * FROM online_transactions
)

SELECT 
    *,
    -- Additional calculated fields
    --revenue_amount - COALESCE(cost_amount, 0) AS gross_margin,
    
    -- Time-based flags for filtering
CASE 
    WHEN transaction_date >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 1) 
    THEN 'MTD'
    WHEN transaction_date >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 1, 1)
        AND transaction_date < DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 1)
    THEN CONCAT(FORMAT_DATE('%b', transaction_date), ' ', RIGHT(CAST(EXTRACT(YEAR FROM transaction_date) AS STRING), 2))
    WHEN EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1
    THEN CONCAT('Year ', RIGHT(CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 1 AS STRING), 2))
    WHEN EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 2
    THEN CONCAT('Year ', RIGHT(CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 2 AS STRING), 2))
    WHEN EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 3
    THEN CONCAT('Year ', RIGHT(CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 3 AS STRING), 2))
    WHEN EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 4
    THEN CONCAT('Year ', RIGHT(CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 4 AS STRING), 2))
    WHEN EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 5
    THEN CONCAT('Year ', RIGHT(CAST(EXTRACT(YEAR FROM CURRENT_DATE()) - 5 AS STRING), 2))
    WHEN EXTRACT(YEAR FROM transaction_date) <= EXTRACT(YEAR FROM CURRENT_DATE()) - 6
    THEN 'Year 19 & Before'
    ELSE 'Unknown'
END AS period_label,

CASE 
    WHEN transaction_date >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 1) THEN 1
    WHEN transaction_date >= DATE(EXTRACT(YEAR FROM CURRENT_DATE()), 1, 1)
        AND transaction_date < DATE(EXTRACT(YEAR FROM CURRENT_DATE()), EXTRACT(MONTH FROM CURRENT_DATE()), 1)
    THEN EXTRACT(MONTH FROM CURRENT_DATE()) - EXTRACT(MONTH FROM transaction_date) + 1
    WHEN EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1
    THEN 13
    WHEN EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 2
    THEN 14
    WHEN EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 3
    THEN 15
    WHEN EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 4
    THEN 16
    WHEN EXTRACT(YEAR FROM transaction_date) = EXTRACT(YEAR FROM CURRENT_DATE()) - 5
    THEN 17
    WHEN EXTRACT(YEAR FROM transaction_date) <= EXTRACT(YEAR FROM CURRENT_DATE()) - 6
    THEN 18
    ELSE 19
END AS period_sort_order,

    -- Reporting metadata
    CURRENT_DATETIME() AS etl_timestamp

FROM combined_transactions

WHERE 
transaction_date BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH), MONTH)
AND LAST_DAY(CURRENT_DATE(), MONTH)



/*
where (transaction_date BETWEEN '2025-01-01' AND '2025-09-30'
       OR transaction_date BETWEEN '2024-12-01' AND '2024-12-31'
       OR transaction_date BETWEEN '2024-01-01' AND '2024-01-31'
      )
*/

