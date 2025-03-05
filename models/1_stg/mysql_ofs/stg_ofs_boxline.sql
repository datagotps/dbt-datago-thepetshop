with 

source as (

    select * from {{ source('h_mysql_ofs', 'boxline') }}

),

renamed as (

    select
        id,
        boxid,
        weborderno,
        itemid,
        batchid,
        itemno,
        insertedon,
        issurface,
        isfragile,
        packid,
        isexchange,
        frequency,
        priority,
        isprecious,
        isgiftwrap,
        boxcreationdatetime,
        forwarddspcode,
        awbno,
        userid,
        invoiceno,
        invoicedate,
        sentforposting,
        picklocation,
        lot,
        senton,
        error,
        tableid,
        routingcode,
        referenceorderno,
        returncreated,
        serialno,
        sku,
        ispdfuploaded,
        qcdoneby,
        barcode,
        isuid,
        __hevo__database_name,
        __hevo__ingested_at,
        __hevo__loaded_at,
        __hevo__marked_deleted,
        __hevo__source_modified_at

    from source

)

select * from renamed
