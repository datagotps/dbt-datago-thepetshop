
select


pl.weborderno,
pl.insertedon AS orderdatetime,

pl.itemid,
od.itemno,


--pl.unitprice,
pl.amount,
pl.tax,
pl.amountincltax,
--pl.discount, --discount apply on header level
pl.mrpprice,

pl.mrpprice - pl.amountincltax  as discount,



id.description as product_name,
id.divisioncodedescription as division,
id.itemcategorycode as item_category,
id.retailproductcode as item_sub_category,
id.brand,


from {{ ref('stg_inboundpaymentline') }} pl
left join {{ ref('stg_orderdetail') }} as od on od.itemid = pl.itemid --and od._fivetran_deleted is false (Feltered in stg level)
left join {{ ref('stg_itemdetail') }} as id on id.itemno = od.itemno  --and id._fivetran_deleted is false and id.id not in (85722,85724,85732,85720,85719,85723,85716,85710,85712) 
 
--left join `Fact.Fact_Item_Master` AS C ON C.itemno = B.itemno

where pl.isheader = 0


--and  pl.weborderno = 'O3585711'
