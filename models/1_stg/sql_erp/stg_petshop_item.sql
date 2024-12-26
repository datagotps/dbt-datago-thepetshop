With source as (
 select * 
 
 
 from {{ source(var('erp_source'), 'petshop_item_5ecfc871_5d82_43f1_9c54_59685e82318d') }} as t1

 FULL OUTER JOIN {{ source(var('erp_source'), 'petshop_item_c91094c2_db03_49d2_8cb9_95c179ecbf9d') }} as t2  ON t1.no_ = t2.no_

)
select 
*,

current_timestamp() as ingestion_timestamp,




from source 