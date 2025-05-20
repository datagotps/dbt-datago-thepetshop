with 

source as (

    select * from {{ source('mysql_ofs', 'ordercount') }}

),

renamed as (

    select
        id,
        locationstatus,
        totalitemcount,
        weborderno,
        customerid,

        contactno,


        
        referenceorderno,
        orderitemcount,
        packaginglocation,
        updatedon,
        updateby,
        insertedon,
        insertedby,
        readyforarchive,
        
        
        merged,
        
        sortingbintype,
        
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed
--where weborderno= 'O30102245S'