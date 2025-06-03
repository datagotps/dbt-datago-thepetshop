with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_item_category_437dbf0e_84ff_417a_965d_ed2bb9650972') }}

),

renamed as (

    select
        code,
        description,
        presentation_order,
    
        
        _systemid,
        has_children,
        id,
        indentation,
        last_modified_date_time,
        parent_category,
        
        _fivetran_deleted,
        _fivetran_synced,
        
        --timestamp

    from source

)

select * from renamed
