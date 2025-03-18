with 

source as (

    select * from {{ source('h_mysql_ofs', 'driverdeliverystage') }}

),

renamed as (

    select
        id,
        awb,
        driverid,
        gatewayid,
        orderpaymentmethod,
        paidamount,
        transactionid,
        autorizeno,
        insertedon,
        insertedby,
        istallied,
        transactionurl,
        isdelivered,
        cashierdocumentno,
        status,
        address1,
        address2,
        lattitude,
        longitude,
        comment,
        proofofpickupimagelink,
        customerimagelink,
        customersignatureimage,
        runsheetno,
        lmsrecordid,
        lmsdriverid,
        deviceid,
        delivereddatetime,
        __hevo__database_name,
        __hevo__ingested_at,
        __hevo__loaded_at,
        __hevo__marked_deleted,
        __hevo__source_modified_at

    from source

)

select * from renamed
