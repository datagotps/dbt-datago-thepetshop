With source as (
 select * from {{ source(var('ofs_source'), 'inboundorderaddress') }}
)
select 
*,

current_timestamp() as ingestion_timestamp,




from source 