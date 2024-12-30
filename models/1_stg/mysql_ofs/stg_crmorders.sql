With source as (
 select * from {{ source(var('ofs_source'), 'crmorders') }}
)
select 
id,
weborderno,
ordertype, --NORMAL, EXPRESS, EXCHANGE
customeremail,
firstname,
customerphone,
orderstatus, --30, 8, 31, 11, 27, 9, 10, 28, 53
packaginglocation, --4, 10, 8, 6, 20, null review: locationmaster table
ordersource, -- D, I, A, CRM, '', CRM Exchange, FOC

lastname,
customerid,



apporderno, 
assigndbyuserid, 
company, 
country, 
currency, 
custdatetime, 
 

customermobile, 
 
customertype, 
 
insertedby, 
insertedon, 
isameyosync, 
isrulerun, 
 
middlename, 
orderamount, 
ordercategory,
orderdatetime, 
 
 

 

 
paymentmethod, 
readyforarchive, 
referenceorderno, 
storeid, 
syncdatetime, 
syncmessage, 
syncupdatedon, 
updatedby, 
updatedon, 
userassign, 
 
websiteorderstatus, 
orderplatform,

_fivetran_deleted, 
_fivetran_synced, 


current_timestamp() as ingestion_timestamp, 




from source 