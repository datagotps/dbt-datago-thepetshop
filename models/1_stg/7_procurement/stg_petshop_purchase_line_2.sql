with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_purchase_line_c91094c2_db03_49d2_8cb9_95c179ecbf9d') }}

),

renamed as (

    select
        document_no_,
        document_type,
        line_no_,
        _fivetran_deleted,
        _fivetran_synced,
        failed_quantity,
        inventory_type,
        passed_quantity,
        qc_done,
        quality_status,
        timestamp,
        fail_quantity,
        qc_by,
        pass_quantity

    from source

)

select * from renamed
