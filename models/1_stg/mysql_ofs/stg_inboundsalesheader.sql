With source as (
 select * from {{ source(var('ofs_source'), 'inboundsalesheader') }}
)
select 
    id,
    weborderno,
    orderplatform, --OCC, shopify, CRM, null
    ordertype, --NORMAL, EXPRESS, EXCHANGE
    paymentgateway, --creditCard, cash, CreditCard, Cash On Delivery, Cash, Pay by Card, StoreCredit, Card on delivery, Cash on delivery, Tabby, Loyalty, PointsPay (Etihad, Etisalat etc.)
    paymentmethodcode, -- PREPAID, COD, creditCard
    ordersource, -- D, I, A, CRM, '', CRM Exchange, FOC

    customerid,


    referenceorderno,
    
    apporderno,
    company,
    confirm,
    confirmationtype, --Normal, Edit
    country, --AE, United Arab Emirates
    customercomment,
    
    errormessage,
    expecteddispatchdate,
    frequency,
    insertedby,
    insertedon,
    isexchange,
    isgiftwrap,
    isorderanalysis,
    issync,
    ordercategory, --NORMAL
    orderconfirmsyncon,
    orderdatetime,
    orderjsonid,
    
    priority,
    readyforarchive,
    
    referrer,
    reservedfield1,
    reservedfield2,
    reservedfield3,
    reservedfield4,
    reservedfield5,
    retrycount,
    storeid,
    updatedby,
    updatedon,
    
    encashamount,

    _fivetran_deleted,
    _fivetran_synced,
current_timestamp() as ingestion_timestamp,




from source 