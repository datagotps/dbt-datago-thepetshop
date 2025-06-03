--70626 (12 may 2025)

with crmorders as (

SELECT 
    customerid,
    STRING_AGG(DISTINCT customerphone, ', ') AS phone_numbers,
    COUNT(DISTINCT customerphone) AS phone_count,
    Min(orderdatetime) as first_online_order_date,
    Max(orderdatetime) as last_online_order_date,
  FROM {{ ref('stg_ofs_crmorders') }}
  GROUP BY customerid

)


select

a.customerid,
a.emailid,

b.phone_count,
b.phone_numbers,
b.first_online_order_date,
b.last_online_order_date,
CASE 
    WHEN b.phone_count = 1 THEN 'Unique Phone'
    WHEN b.phone_count > 1 THEN 'Multiple Phones'
    WHEN b.phone_count = 0 OR b.phone_count IS NULL THEN 'No Phone'
    ELSE 'Invalid Data'
END AS phone_status,

--c.raw_phone_no_,
--c.std_phone_no_,

from {{ ref('stg_ofs_customerdetails') }} as a
left join crmorders as b on a.customerid =b.customerid