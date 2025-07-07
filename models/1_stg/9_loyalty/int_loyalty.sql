SELECT
memberId,
max(phone) as phone ,
max (registeredAt) as registeredAt,
    
from {{ ref('stg_loyalty_members') }} as a
--left join  {{ ref('int_erp_customer') }}  as b on a.phone = b.std_phone_no_
group by memberId