
/*

select
count(*)

From {{ ref('stg_crmorders') }} as C
left join {{ ref('stg_inboundsalesheader') }}  A on A.WebOrderNo = C.WebOrderNo
join {{ ref('stg_inboundorderaddress') }}  B on A.WebOrderNo = B.WebOrderNo
join  {{ ref('stg_orderstatusmaster') }}  D on C.OrderStatus = D.Id
join {{ ref('stg_inboundpaymentline') }}  E on A.WebOrderNo = E.WebOrderNo