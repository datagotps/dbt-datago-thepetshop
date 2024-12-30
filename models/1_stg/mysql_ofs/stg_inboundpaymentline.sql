With source as (
 select * from {{ source(var('ofs_source'), 'inboundpaymentline') }}
)
select 
id,
weborderno,


agentcode,
agentcommission, 
amount, 
amountincltax, 
authorizationid, 
celebrityordersync,
codcharges,
couponamount,
couponcode,
currencycode,
currencyfactor,
customduty,
customdutypercentage,
customizedcharges,
customthresholdlimit,
discount,
discounttype,
errormessage,
giftcharges,
insertedby,
insertedon,
invoicediscountamount,
invoicediscounttax,
invoicediscountwithtax,
isheader,
isrecalculated,
itemid,
loyalitypointamount,
loyaltypoints,
mrpprice,
orderchargesprocessed,
othercharges,
paymentgateway,
paymentgateway2,
paymentgateway3,
paymentgateway4,
paymentgatewayamount,
paymentgatewayamount2,
paymentgatewayamount3,
paymentgatewayamount4,
paymentmethodcode,

readyforarchive,
recalculatedamount,
retrycounter,
roundingdifference,
shippingcharges,
storecredit,
tax,
taxpercentage,
transactionid,
unitprice,
unitpriceincludingtax,
updatedby,
updatedon,
walletamount,
walletname,



_fivetran_deleted,
_fivetran_synced,

current_timestamp() as ingestion_timestamp,




from source 