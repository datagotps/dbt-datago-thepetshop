With source as (
 select * from {{ source(var('ofs_source'), 'inboundpaymentline') }}
)
select 
*,

current_timestamp() as ingestion_timestamp,




from source 