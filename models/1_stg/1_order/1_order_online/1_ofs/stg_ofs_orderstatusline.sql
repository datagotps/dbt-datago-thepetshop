with 

source as (

    select * from {{ source('h_mysql_ofs', 'orderstatusline') }}

),

renamed as (

    select
        id,
        insertedon,
        weborderno,
        itemid,
        statusid,
        statusname,

        readyforarchive,
        
        insertedby,
        updatedon,
        updatedby,
        source,
        __hevo__database_name,
        __hevo__ingested_at,
        __hevo__loaded_at,
        __hevo__marked_deleted,
        __hevo__source_modified_at

    from source

),

deduplicated as (

    select
        *,
        ROW_NUMBER() OVER (
            PARTITION BY weborderno, itemid, statusid, statusname
            ORDER BY insertedon, id  -- Added id as a tiebreaker for identical timestamps
        ) AS status_rank
    from renamed

)

select * except(status_rank) 
from deduplicated
where status_rank = 1