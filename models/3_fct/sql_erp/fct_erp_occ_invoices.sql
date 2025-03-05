With source as (
 select * from {{ ref('int_erp_occ_invoices') }}
)
select 

*
from source 

