WITH online_customers_ranked AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY web_customer_no_ 
            ORDER BY 
                date_created ASC,  -- Oldest record first
                no_ ASC            -- If same date, use lowest ID
        ) AS rn,
        COUNT(*) OVER (PARTITION BY web_customer_no_) AS id_count  -- Count of ERP IDs for the same web_customer_no_
    FROM {{ ref('1_stg_erp_customer') }}
    WHERE web_customer_no_ IS NOT NULL 
      AND web_customer_no_ != ''
),

offline_customers AS (
    SELECT 
        *,
        1 AS rn,  -- All offline customers have rn = 1
        1 AS id_count,  -- No duplicates for offline customers
        FALSE AS has_multiple_erp_ids
    FROM {{ ref('1_stg_erp_customer') }}
    WHERE web_customer_no_ IS NULL 
       OR web_customer_no_ = ''
),

all_customers AS (
    -- Deduplicated online customers
    SELECT 
        * EXCEPT(id_count, rn),  
        CASE 
            WHEN id_count > 1 THEN TRUE 
            ELSE FALSE 
        END AS has_multiple_erp_ids
    FROM online_customers_ranked
    WHERE rn = 1
    
    UNION ALL
    
    -- All offline customers (no deduplication needed)
    SELECT 
        * EXCEPT(id_count, rn, has_multiple_erp_ids),
        has_multiple_erp_ids
    FROM offline_customers
)

SELECT * FROM all_customers

