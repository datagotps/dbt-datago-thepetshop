with 

source as (

    select * from {{ source('mysql_ofs', 'crmlinestatus') }}

),

renamed as (

    select
        id,
        weborderno,
        itemid,
        statusid,
        statusname,
        insertedon,
        insertedby,
        updatedon,
        updatedby,
        source,
        _fivetran_deleted,
        _fivetran_synced

    from source

)

select * from renamed
