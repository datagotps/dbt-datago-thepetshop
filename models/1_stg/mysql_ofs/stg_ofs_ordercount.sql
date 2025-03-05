with 

source as (

    select * from {{ source('h_mysql_ofs', 'ordercount') }}

),

renamed as (

    select
        weborderno,
        locationstatus,
        
        referenceorderno,
        orderitemcount,
        packaginglocation,
        updatedon,
        updateby,
        insertedon,
        insertedby,
        readyforarchive,
        customerid,
        contactno,
        merged,
        totalitemcount,
        sortingbintype,
        
        __hevo__database_name,
        __hevo__ingested_at,
        __hevo__loaded_at,
        __hevo__marked_deleted,
        __hevo__source_modified_at

    from source

)

select * from renamed
