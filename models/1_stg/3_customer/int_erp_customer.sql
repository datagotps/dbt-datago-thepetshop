select


a.no_,
a.phone_no_,
c.membership_card_no_,
c.store_credit_gift_no_,
CASE
    WHEN REGEXP_CONTAINS(a.no_, r'^(CUST|CDN|BCN)') THEN 'Deferred (CUST|CDN|BCN)'
    WHEN a.phone_no_ IS NULL OR a.phone_no_ = '' THEN 'Missing Phone Number'
    WHEN REGEXP_CONTAINS(a.phone_no_, r'^5[0-9]{8}$') THEN 'Valid - Standard (9 digits starting with 5)'  --9 digits starting with 5 --545110418.
    WHEN REGEXP_CONTAINS(a.phone_no_, r'^971[0-9]{9}$') THEN 'Valid - Needs Trim (12 digits With 971 Prefix)' --Starts with 971 + 9 digits (12 total) 
    WHEN REGEXP_CONTAINS(a.phone_no_, r'^05[0-9]{8}$') THEN 'Valid - Needs Trim (10 digits starting with 05)' --10 digits starting with 05 -  Ex: 0507618945
    
    WHEN c.phone_country_code IS NOT NULL  AND c.phone_country_code NOT IN ('971', '+971') THEN 'Non-UAE Country'

    ELSE 'Invalid - Pattern Error'
END AS phone_no_status,


c.phone_country_code,

a.name,
a.name_2,

a.country_region_code,

c.source_application,


a.e_mail,
a.gen__bus__posting_group,

b.created_by_user,
date(b.date_created) as date_created ,


from  {{ ref('stg_erp_customer_1_972') }} as a
 left join {{ ref('stg_erp_customer_2_18d') }} as b on a.no_ = b.no_
 left join {{ ref('stg_erp_customer_3_f9d') }} as c on a.no_ = c.no_
 left join {{ ref('stg_erp_customer_4_531') }} as d on a.no_ = d.no_

 --251399

 --where c.anmar_phone is null

