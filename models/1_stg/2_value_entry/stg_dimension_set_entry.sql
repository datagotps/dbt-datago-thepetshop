with 

source as (
    
    select * from {{ source('sql_erp_prod_dbo', 'petshop_dimension_set_entry_437dbf0e_84ff_417a_965d_ed2bb9650972') }}

),

renamed as (

    select
        dimension_code,
        dimension_set_id,
        _fivetran_deleted,
        _fivetran_synced,
        dimension_value_code,
        _systemid,
        dimension_value_id,
        timestamp

    from source

)

select * from renamed
