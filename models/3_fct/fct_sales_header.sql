With source as (
 select * from {{ ref('int_sales_header') }}
)
select 

*
from source 

