with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_item_sub_category_c91094c2_db03_49d2_8cb9_95c179ecbf9d') }}

),

renamed as (

    select
        category_code,
        code,
        division_code,
        retail_product_code,
        description,
        no__series, 
        _Systemid, 
        timestamp,
        _fivetran_deleted,
        _fivetran_synced,

    from source

)

select * from renamed
