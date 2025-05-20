with 

source as (

    select * from {{ source('mysql_ofs', 'locationmaster') }}

),

renamed as (

    select
        id,
        location,
        description,


        _fivetran_deleted,
        _fivetran_synced,
        address1,
        address2,
        address3,
        cancelputawayrequire,
        cellphonenumber,
        city,
        companyemailid,
        companyname,
        country,
        
        directprintflag,
        doublehitkey,
        doublehitrequired,
        failbin,
        failedbin,
        faxnumber,
        glaccountnumber,
        isactive,
        isdelivery,
        isfasttrack,
        ispickup,
        itemscanbox,
        
        numberofdays,
        passbin,
        passedbin,
        phonenumber1,
        phonenumber2,
        pickupsafetystock,
        pnrbin,
        postcode,
        refurnishbin,
        returnfailputawayrequire,
        returnpassputawayrequire,
        shipbin,
        sortingalgo,
        state,
        stockpriority,
        storelocationurl,
        timezoneoffset,
        trnno,
        type,
        defaultdriverid,
        shopifylocationid

    from source

)

select * from renamed
