with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_purchase_header_c91094c2_db03_49d2_8cb9_95c179ecbf9d') }}

),

renamed as (

    select
        document_type,
        no_,
        _fivetran_deleted,
        _fivetran_synced,
        completely_qc_done,
        created_by_user,
        drop_ship_order,
        expense_amount,
        expense_template_code,
        operation_types,
        order_no_,
        purchase_type,
        qc_started,
        quality_status,
        short_close,
        timestamp,
        isslim4sync,
        slimstock_document_no

    from source

)

select * from renamed
