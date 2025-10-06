with 

source as (
    select * from {{ source('mysql_ofs', 'inboundsalesheader') }}
    where _fivetran_deleted is false

),

renamed as (
    select
        -- Primary Identifiers
        id,
        apporderno,
        weborderno,
        referenceorderno,
        orderjsonid,
        
        -- System/Metadata Fields
        _fivetran_deleted,
        _fivetran_synced,
        insertedby,
        insertedon,
        updatedby,
        updatedon,
        retrycount,
        readyforarchive,
        
        -- Company & Location
        company,
        storeid,
        country,
        
        -- Customer Information
        customerid,
        customercomment,
        
        -- Order Classification
        ordercategory,
        ordertype, -- NORMAL, EXPRESS, EXCHANGE
        ordersource,
        
        case
            when ordersource = 'D' then 'Website'
            when ordersource in ('CRM', 'CRM Exchange', 'FOC') then 'CRM'
            when ordersource in ('A') then 'Android'
            when ordersource in ('I') then 'iOS'
            when ordersource = '' AND orderdatetime >= DATE '2024-10-01' AND orderdatetime <  DATE '2024-11-01' THEN 'Website'
            when ordersource = '' AND referrer LIKE '%.%' THEN 'iOS'
            else ordersource
        end as online_order_channel,

        case 
            when orderplatform = '' and referrer = 'CRM' then 'CRM'
            else orderplatform
        end as orderplatform,
        
        referrer,
        
        -- Payment Information
        paymentgateway, -- cash, CreditCard, Cash On Delivery, Pay by Card, StoreCredit, Card on delivery, Tabby, Loyalty, PointsPay (Etihad, Etisalat etc.)
        paymentmethodcode, -- PREPAID, COD, creditCard
        encashamount,
        
        -- Order Processing & Status
        confirm,
        confirmationtype,
        orderconfirmsyncon,
        orderdatetime as order_date,
        expecteddispatchdate,
        priority,
        frequency,
        errormessage,
        
        -- Order Features/Flags
        isexchange,
        isgiftwrap,
        isorderanalysis,
        issync,
        
        -- Reserved/Future Use Fields
        reservedfield1,
        reservedfield2,
        reservedfield3,
        reservedfield4,
        reservedfield5

    from source
)

select * from renamed

--where weborderno= 'O30163245S'