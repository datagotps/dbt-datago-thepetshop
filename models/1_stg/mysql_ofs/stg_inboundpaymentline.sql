With source as (
 select * from {{ source(var('ofs_source'), 'inboundpaymentline') }}
)
select 
id, --Unique identifier (primary key) for each payment line record.

weborderno,
insertedon, --DateTime when this record was inserted.   as OrderDate,



transactionid,
itemid, --Identifier for the item (product) in the line.
authorizationid,


amount, --Base amount (excluding tax).
amountincltax, --Amount including tax (the total price after tax).
tax, --Tax amount on this payment line.
couponamount, --Amount deducted due to coupons.
unitprice, --Base unit price (excluding tax) for the item.
unitpriceincludingtax, --Unit price including tax.

couponcode, --Code of the applied coupon.
discount, --Discount amount applied to this payment line.
storecredit, --Amount of store credit used.






--charges
    othercharges, --Miscellaneous charges (e.g., additional fees).
    customizedcharges, --Additional custom charges that may apply to the order.
    shippingcharges, --Shipping charges applied.
    codcharges, --Cash on Delivery charges applied to the order.
    giftcharges, --Charges for gift wrapping or gift services.



loyalitypointamount, --Monetary equivalent of loyalty points applied to this line.
loyaltypoints, --Number of loyalty points applied or earned.


 
agentcode,
agentcommission, 
 
 
 
celebrityordersync,




currencycode, --Code of the currency used (e.g., “AED”, “USD”).
currencyfactor, --Exchange rate factor for the currency at the time of the transaction.

customduty, --Custom duty charges applied to the order (if any).
customdutypercentage, --Percentage used to calculate custom duty.
customthresholdlimit, --Threshold limit for custom duties or other conditions.




discounttype, --Type of discount (e.g., “Percent” or “Fixed”).
errormessage, --Error or exception message encountered during processing.



insertedby, --Username or process ID that inserted this record.


invoicediscountamount,  --Discount amount applicable at the invoice level.

invoicediscounttax,
invoicediscountwithtax,
isheader,
isrecalculated,


mrpprice,
orderchargesprocessed,

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



taxpercentage,





updatedby,
updatedon,
walletamount,
walletname,



_fivetran_deleted,
_fivetran_synced,

current_timestamp() as ingestion_timestamp,




from source 