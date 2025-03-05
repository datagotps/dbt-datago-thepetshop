With source as (
 select * from {{ ref('int_occ_orders') }}
)
select 

*
from source 

