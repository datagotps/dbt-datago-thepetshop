with 

source as (
    select * from {{ source('sql_erp_prod_dbo', 'petshop_pna_details_c91094c2_db03_49d2_8cb9_95c179ecbf9d') }}
),

renamed as (
    select
        sync_id,
        inserted_by,
        
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
),

-- First identify duplicated item_ids, but exclude item_id = 0
item_id_counts as (
    select 
        item_id,
        count(*) as item_count
    from renamed
    where item_id != 0  -- Exclude item_id = 0 from deduplication
    group by item_id
    having count(*) > 1
),

-- Apply row_number only to duplicated item_ids that are not 0
deduplicated as (
    select
        r.*,
        case
            when ic.item_id is not null then
                row_number() over(
                    partition by r.item_id
                    order by 
                        r.updated_at desc nulls last,
                        r.insert_date_time desc nulls last,
                        r._systemid
                )
            else 1 -- Keep all records for item_id = 0 and non-duplicated item_ids
        end as row_num
    from renamed r
    left join item_id_counts ic
        on r.item_id = ic.item_id
)

-- Keep all records where row_num = 1
select * except(row_num)
from deduplicated
where row_num = 1

--where source_no_


--WHERE document_no_ LIKE 'O%'
--where document_no_ = 'O30102245S'
