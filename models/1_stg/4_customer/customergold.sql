WITH 
-- Step 1: Group customers by email first
email_grouping AS (
  SELECT 
    e_mail,
    DENSE_RANK() OVER (ORDER BY e_mail) AS global_customer_id,
    STRING_AGG(DISTINCT no_, ', ') AS erp_ids,
    STRING_AGG(DISTINCT CAST(raw_phone_no_ AS STRING), ', ') AS phone_numbers,
    STRING_AGG(DISTINCT CAST(web_customer_no_ AS STRING), ', ') AS web_customer_ids,
    COUNT(DISTINCT no_) AS erp_count,
    COUNT(DISTINCT raw_phone_no_) AS phone_count,
    COUNT(DISTINCT web_customer_no_) AS web_count
  FROM {{ ref('1_stg_erp_customer') }}
  WHERE e_mail IS NOT NULL AND TRIM(e_mail) != ''
  GROUP BY e_mail
),

-- Get records that were matched by email
matched_by_email AS (
  SELECT DISTINCT 
    c.no_,
    c.raw_phone_no_,
    c.e_mail,
    c.web_customer_no_,
    eg.global_customer_id
  FROM {{ ref('1_stg_erp_customer') }} c
  JOIN email_grouping eg ON c.e_mail = eg.e_mail
  WHERE c.e_mail IS NOT NULL AND TRIM(c.e_mail) != ''
),

-- Step 2: Find remaining records not matched by email
unmatched_records AS (
  SELECT *
  FROM {{ ref('1_stg_erp_customer') }}
  WHERE (e_mail IS NULL OR TRIM(e_mail) = '')
     OR no_ NOT IN (SELECT no_ FROM matched_by_email)
),

-- Group remaining records by phone number
phone_grouping AS (
  SELECT 
    raw_phone_no_,
    DENSE_RANK() OVER (ORDER BY raw_phone_no_) + (SELECT MAX(global_customer_id) FROM email_grouping) AS global_customer_id,
    STRING_AGG(DISTINCT no_, ', ') AS erp_ids,
    STRING_AGG(DISTINCT CAST(e_mail AS STRING), ', ') AS emails,
    STRING_AGG(DISTINCT CAST(web_customer_no_ AS STRING), ', ') AS web_customer_ids,
    COUNT(DISTINCT no_) AS erp_count,
    COUNT(DISTINCT e_mail) AS email_count,
    COUNT(DISTINCT web_customer_no_) AS web_count
  FROM unmatched_records
  WHERE raw_phone_no_ IS NOT NULL AND TRIM(raw_phone_no_) != ''
  GROUP BY raw_phone_no_
),

-- Get records matched by phone
matched_by_phone AS (
  SELECT DISTINCT
    u.no_,
    u.raw_phone_no_,
    u.e_mail,
    u.web_customer_no_,
    pg.global_customer_id
  FROM unmatched_records u
  JOIN phone_grouping pg ON u.raw_phone_no_ = pg.raw_phone_no_
  WHERE u.raw_phone_no_ IS NOT NULL AND TRIM(u.raw_phone_no_) != ''
),

-- Combine all matched records
all_matched AS (
  SELECT * FROM matched_by_email
  UNION ALL
  SELECT * FROM matched_by_phone
),

-- Create final customer master with aggregated data
customer_master AS (
  SELECT 
    global_customer_id,
    STRING_AGG(DISTINCT CAST(no_ AS STRING), ', ') AS all_erp_ids,
    STRING_AGG(DISTINCT CAST(e_mail AS STRING), ', ') AS all_emails,
    STRING_AGG(DISTINCT CAST(raw_phone_no_ AS STRING), ', ') AS all_phone_numbers,
    STRING_AGG(DISTINCT CAST(web_customer_no_ AS STRING), ', ') AS all_web_customer_ids,
    COUNT(DISTINCT no_) AS total_erp_accounts,
    COUNT(DISTINCT e_mail) AS total_emails,
    COUNT(DISTINCT raw_phone_no_) AS total_phone_numbers,
    COUNT(DISTINCT web_customer_no_) AS total_web_accounts,
    MIN(no_) AS primary_erp_id -- You might want to choose based on other criteria
  FROM all_matched
  GROUP BY global_customer_id
)

-- Final output
SELECT * FROM customer_master
ORDER BY global_customer_id
