With source as (
 select * from {{ ref('int_erp_sales_invoice_header') }}
)
select 

*
from source 

