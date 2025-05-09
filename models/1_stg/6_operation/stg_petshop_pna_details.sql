with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_pna_details_c91094c2_db03_49d2_8cb9_95c179ecbf9d') }}

),

renamed as (

    select
        sync_id,
        _fivetran_deleted,
        _fivetran_synced,
        _systemid,
        assign_user_id,
        barcode,
        batch_id,
        bin_code,
        complete,
        count,
        document_line_no_,
        document_no_,
        insert_date_time,
        inserted_by,
        item_id,
        item_no,
        lot_no_,
        ready_for_pick,
        serial_no,
        source_no_,
        source_type,
        sync_to_wms,
        ticket_created,
        ticket_id,
        timestamp,
        updated_at,
        updated_by,
        zone_code

    from source

)

select * from renamed
