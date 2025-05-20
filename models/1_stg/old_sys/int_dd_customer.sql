select
distinct
E_Mail,
STRING_AGG(DISTINCT CAST(Phone_No AS STRING), ', ') AS phone_numbers,
  COUNT(DISTINCT Phone_No) AS phone_count,
  max(Phone_No) as max_contactdetails_phone,
FROM {{ ref('stg_dd_customer') }} 
WHERE email_status = 'Valid Email'

GROUP BY E_Mail
