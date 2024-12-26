With source as (
 select * from {{ source(var('erp_source'), 'petshop_replenishment_mapping_c91094c2_db03_49d2_8cb9_95c179ecbf9d') }}
)
select 
*,

current_timestamp() as ingestion_timestamp,




from source 