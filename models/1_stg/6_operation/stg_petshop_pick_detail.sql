with 

source as (

    select * from {{ source('sql_erp_prod_dbo', 'petshop_pick_detail_c91094c2_db03_49d2_8cb9_95c179ecbf9d') }}

),

renamed as (

    select
        orderno,
        bin,
        insertdatetime,

        item_type,
        businessoperationtype,

        processed_quantity,
        quantity,
        itemno,

        sync_id,
        _fivetran_deleted,
        _fivetran_synced,
        _systemid,
        actual_bin,
        actual_zone,
        barcode,
        batchid,
        
        bin_2,
        bin_3,
        bin_4,
        bin_5,
        bin_size,
        
        error_message,
        image_url,
        
        inventory_type,
        
        itemid,
        
        lot_no,
        ofssynced,
        ofssynceddatetime,
        ofssyncederror,
        order_item_barcode,
        
        ordertype, --EXPRESS, EXCHANGE, NORMAL, NULL
        pickcreated,
        pickcreationdatetime,
        picking_type,
        pickinglocation,
        pickno,
        pickregistered,
        
        registrationdatetime,
        registrationuserid,
        retry_count,
        serial_no_,
        sortingbin,
        source_line_no_,
        source_no_,
        source_sub_document_no,
        source_subline_no_,
        source_type,
        timestamp,
        tracking,
        wms_user,
        wmssynced,
        wmssynceddatetime,
        zone,
        zone_2,
        zone_3,
        zone_4,
        zone_5,
        packaging_location

    from source

)

select * from renamed

--where bin = ''