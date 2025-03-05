With source as (
 select * from {{ source(var('erp_source'), 'petshop_staff_5ecfc871_5d82_43f1_9c54_59685e82318d') }}
)
select 
*,

current_timestamp() as ingestion_timestamp,




from source 