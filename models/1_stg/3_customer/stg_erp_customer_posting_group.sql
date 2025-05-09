with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_customer_posting_group_c91094c2_db03_49d2_8cb9_95c179ecbf9d') }}

),

renamed as (

    select
        code,
        _fivetran_deleted,
        _fivetran_synced,
        slimstock_posting_group,
        slimstock_posting_group_pos,
        slimstock_posting_type,
        timestamp

    from source

)

select * from renamed
