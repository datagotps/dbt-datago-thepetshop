select

dse.dimension_code, 
dse.dimension_value_code, 
dse.dimension_value_id,
dse.dimension_set_id,

dv.name,
dv.code,
dv.last_modified_date_time,
dv.indentation,
dv.global_dimension_no_,
dv.dimension_value_type,
dv.blocked,


FROM {{ ref('stg_dimension_set_entry') }} as dse
LEFT JOIN  {{ ref('stg_dimension_value') }}  as dv  on dse.dimension_value_id = dv.dimension_value_id
