with 

source as (

    select * from {{ source('mysql_ofs', 'ordercount') }}

),

renamed as (

    select
        id,
        weborderno,
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
        locationstatus,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed
