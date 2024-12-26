With source as (
 select * from {{ source(var('erp_source'), 'petshop_customer_437dbf0e_84ff_417a_965d_ed2bb9650972') }}
)
select 
*,

current_timestamp() as ingestion_timestamp,




from source 