with source as (select * from {{ source(var("ofs_source"), "inboundsalesheader") }})
select


id,

--Order
    weborderno,
    referenceorderno,
    orderplatform,  -- OCC, shopify, CRM, null
    ordercategory,  -- NORMAL
    orderdatetime,
    ordertype,  -- NORMAL, EXPRESS, EXCHANGE
    paymentmethodcode,  -- PREPAID, COD, creditCard
    paymentgateway,  -- creditCard, cash, CreditCard, Cash On Delivery, Cash, Pay by Card, StoreCredit, Card on delivery, Cash on delivery, Tabby, Loyalty, PointsPay (Etihad, Etisalat etc.)


--ordersource,  -- D, I, A, CRM, '', CRM Exchange, FOC

case
when ordersource = 'D' then 'Website'
when ordersource in ('CRM', 'CRM Exchange', 'FOC') then 'CRM'
when ordersource in ('A') then 'Android'
when ordersource in ('I') then 'iOS'
else ordersource
end as ordersource,





    expecteddispatchdate,

    referrer,

    reservedfield1,
    reservedfield2,
    reservedfield3,
    reservedfield4,
    reservedfield5,




-- Customer
    customerid,
    customercomment,





apporderno,
company,
confirm,
confirmationtype,  -- Normal, Edit
country,  -- AE, United Arab Emirates
errormessage,
frequency,
insertedby,
insertedon,
isexchange,
isgiftwrap,
isorderanalysis,
issync,
orderconfirmsyncon, --null
orderjsonid,
priority,
readyforarchive,
retrycount,
storeid,
updatedby,
updatedon,
encashamount,

_fivetran_deleted,
_fivetran_synced,
current_timestamp() as ingestion_timestamp,

from source
