SELECT 
    web_customer_no_,
    COUNT(*) as duplicate_count,
    STRING_AGG(DISTINCT created_by_user, ', ' ORDER BY created_by_user) as created_by_users,
    STRING_AGG(DISTINCT CAST(DATE(date_created) AS STRING), ', ' ORDER BY CAST(DATE(date_created) AS STRING)) as created_dates,
    MIN(no_) as min_customer_no,
    MAX(no_) as max_customer_no
FROM {{ ref('1_stg_erp_customer') }}
WHERE web_customer_no_ IS NOT NULL 
  AND web_customer_no_ != ''
GROUP BY web_customer_no_
HAVING COUNT(*) > 1