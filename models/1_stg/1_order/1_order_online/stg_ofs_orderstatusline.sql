with 

source as (

    select * from {{ source('h_mysql_ofs', 'orderstatusline') }}

),

renamed as (

    select
        id,
        weborderno,
        itemid,
        statusid,
        statusname,
        readyforarchive,
        insertedon,
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

)

select * from renamed
