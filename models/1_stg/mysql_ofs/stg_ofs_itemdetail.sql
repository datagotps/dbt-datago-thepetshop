With source as (
 select * from {{ source(var('ofs_source'), 'itemdetail') }}
)
select 

*
from source 

where _fivetran_deleted is false and id not in (85722,85724,85732,85720,85719,85723,85716,85710,85712)