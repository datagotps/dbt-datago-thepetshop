With source as (
 select * from {{ source(var('mysql_ofs'), 'inboundsalesheader') }}
)
select 
*,

current_timestamp() as ingestion_timestamp,




from source 