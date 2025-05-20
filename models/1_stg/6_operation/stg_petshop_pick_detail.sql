with 

source as (
    select * from {{ source('sql_erp_prod_dbo', 'petshop_pick_detail_c91094c2_db03_49d2_8cb9_95c179ecbf9d') }}
),

renamed as (
    select
        orderno,
        bin,
        
        itemid,
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
        lot_no,
        ofssynced,

        ofssynceddatetime,
        insertdatetime,
        pickcreationdatetime,
        registrationdatetime,
        wmssynceddatetime,
        
        ofssyncederror,
        order_item_barcode,
        ordertype, --EXPRESS, EXCHANGE, NORMAL, NULL
        pickcreated,
        
        picking_type,
        pickinglocation,
        pickno,
        pickregistered,
        
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
        
        zone,
        zone_2,
        zone_3,
        zone_4,
        zone_5,
        packaging_location
    from source
),

-- Apply deduplication logic: keep all records where itemid = 0,
-- and deduplicate records where itemid != 0
deduplicated as (
    select
        *,
        -- Only assign row numbers for deduplication when itemid != 0
        case
            when itemid != 0 then
                row_number() over(
                    partition by itemid
                    order by 
                        -- Prioritize records with non-null timestamps (if relevant)
                        case when timestamp is null then 0 else 1 end desc,
                        -- Then prioritize by insertion datetime (most recent first if you want latest record)
                        insertdatetime desc,
                        -- Then any other fields that help determine which record to keep
                        _systemid
                )
            else 1 -- For itemid = 0, all records get row_num = 1 (keep all)
        end as row_num
    from renamed
)

-- Final selection: keep only the first occurrence of each itemid when itemid != 0
-- and keep all records when itemid = 0
select
    * except(row_num)
from deduplicated
where row_num = 1

and _fivetran_deleted is not true 

--where orderno = 'O30102245S'



--item_type = 'Item'
-- itemid = 0   itemid != 0
--7294166    +   4991591

--where itemid != 0 --4991591



--7294166 - where itemid = 0
    --6515498  - WHERE orderno LIKE 'TO%' TO/24/009443
    --115546  - WHERE orderno  LIKE 'PR%' PR/2022/000157
    --662891 where orderno   LIKE 'SO%' SO/2021/00247
    --231 where orderno   LIKE 'BCN%'  BCN/2021/000063
    --6515498 + 115546 + 662891 + 231 = 7294166