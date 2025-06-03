

select
a.orderno,
a.bin,
a.quantity,
a.processed_quantity,
a.itemno,

b.shipment_date,
b.transfer_from_code,
b.transfer_to_code,


case when a.bin =''  then a.quantity else 0 end as not_captured_qty,
case when a.bin !=''  then a.quantity else 0 end as captured_qty,
case when a.bin !='' and a.processed_quantity = 1 then a.quantity else 0 end as picked_qty,
case when a.bin !='' and a.processed_quantity = 0 then a.quantity else 0 end as  not_picked_qty,

FROM {{ ref('stg_petshop_pick_detail') }}  as a
inner join {{ ref('stg_petshop_transfer_header') }}  as b on a.orderno = b.no_

WHERE  transfer_from_code in ('HQW','HQW2') 
--and b.shipment_date = DATE '2025-05-03'  and transfer_to_code = 'DIP'