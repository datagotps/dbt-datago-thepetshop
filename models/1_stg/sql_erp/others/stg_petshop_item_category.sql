with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_item_category_437dbf0e_84ff_417a_965d_ed2bb9650972') }}

),

renamed as (

    select
        code,
        _fivetran_deleted,
        _fivetran_synced,
        _systemid,
        description,
        has_children,
        id,
        indentation,
        last_modified_date_time,
        parent_category,
        presentation_order,
        timestamp

    from source

)

select * from renamed
