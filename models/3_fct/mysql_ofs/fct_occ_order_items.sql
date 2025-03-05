With source as (
 select * from {{ ref('int_occ_order_items') }}
)
select 

*
from source 

