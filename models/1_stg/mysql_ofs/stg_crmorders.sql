With source as (
 select * from {{ source(var('ofs_source'), 'crmorders') }}
)
select 
id,


--Order
    weborderno,
    referenceorderno, 
    orderplatform, --OCC, shopify, CRM, null
    ordercategory, -- NORMAL
    orderdatetime, 
    ordersource, -- D, I, A, CRM, '', CRM Exchange, FOC
    ordertype, --NORMAL, EXPRESS, EXCHANGE
    paymentmethod,  --PREPAID, COD, null
    currency, --AED

    -- NOt in inboundsalesheader
    orderamount,
    orderstatus, --30, 8, 31, 11, 27, 9, 10, 28, 53
    packaginglocation, --4, 10, 8, 6, 20, null review: locationmaster table

--Customer
    customerid,
    firstname,
    middlename, 
    lastname,
    customeremail,
    customerphone,
    customertype, --NORMAL, RISKY, FRAUD, BLACKLISTED



-- Not Useed

case
when PackagingLocation = 4 then 'Online Delivery'
else 'Click & Collect'
end as orderdeliverytype,



customermobile, -- null

apporderno, 
assigndbyuserid, 
company, --1
country, --AE
custdatetime, 
insertedby, 
insertedon, 
isameyosync, 
isrulerun, 
userassign, 

readyforarchive, --0
storeid,  --all en
syncdatetime, --null
syncmessage, --null 
websiteorderstatus, --null
syncupdatedon, 
updatedby, 
updatedon, 

_fivetran_deleted, 
_fivetran_synced, 


current_timestamp() as ingestion_timestamp, 




from source 