with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_dimension_value_437dbf0e_84ff_417a_965d_ed2bb9650972') }}

),

renamed as (

    select
        code,
        name,
        dimension_code,
        
        
        _systemid,
        blocked,
        consolidation_code,
        dimension_value_id,
        dimension_value_type,
        global_dimension_no_,
        id,
        indentation,
        last_modified_date_time,
        map_to_ic_dimension_code,
        map_to_ic_dimension_value_code,
        
        timestamp,
        totaling,
        _fivetran_deleted,
        _fivetran_synced,

    from source

)

select * from renamed
