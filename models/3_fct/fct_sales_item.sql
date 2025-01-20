With source as (
 select * from {{ ref('int_sales_item') }}
)
select 

*
from source 

