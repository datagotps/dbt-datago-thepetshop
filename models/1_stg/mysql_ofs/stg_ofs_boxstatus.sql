with 

source as (

    select * from {{ source('h_mysql_ofs', 'boxstatus') }}

),

renamed as (

    select
        id,
        boxid,
        awbno,
        weborderno,
        referenceorderno,
        invoiceno,
        isboxsync,
        boxsyncdate,
        boxretrycount,
        boxerrormessage,
        isship,
        shipdatetime,
        isshipsync,
        shipsyncdate,
        shipretrycount,
        shiperrormessage,
        isdelivered,
        delivereddatetime,
        isdeliveredsync,
        deliveredsyncdate,
        deliveredretrycount,
        deliverederrormessage,
        ispaymentcollected,
        paymentdatetime,
        ispaymentcollectedsync,
        paymentcollectedsyncdate,
        paymentretrycount,
        paymenterrormessage,
        isnpssync,
        npssyncdate,
        npsretrycount,
        npserrormessage,
        forwarddspcode,
        readyforarchive,
        insertedon,
        insertedby,
        returnbeforedelivery,
        isupdateawb,
        awbupdatedby,
        awbupdatedon,
        sourcetype,
        shipmentstatussyncon,
        zonecode,
        comment,
        proofofpickupimagelink,
        customerimagelink,
        customersignatureimage,
        trackinglink,
        isshopifysync,
        shopifysyncretry,
        shopifysyncon,
        __hevo__database_name,
        __hevo__ingested_at,
        __hevo__loaded_at,
        __hevo__marked_deleted,
        __hevo__source_modified_at

    from source

)

select * from renamed
