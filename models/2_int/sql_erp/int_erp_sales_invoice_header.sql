
-- LOD is INV per raw



with
invoice_line as (
    select
        document_no_,
        sum(amount) as amount,
        sum(amount_including_vat) as amount_including_vat,
        count(*) as invoice_item_count,
       
    from {{ ref('stg_erp_sales_invoice_line') }}
    group by 
        1     
)       




select

b.web_order_id,
a.no_,
 

case when c.order_date is not null then c.order_date  else  a.order_date end as  order_date,

ii.amount_including_vat,
ii.amount,
ii.invoice_item_count,

b.type,
/*
a.posting_date,
a.document_date,
a.due_date,
*/

case 
    when c.orderplatform ='' and c.referrer = 'CRM' then 'CRM'
    else c.orderplatform
    end as orderplatform,

case when a.posting_description like 'Order INV%' then 'Online Order' else 'Offline Order' end as sales_channel,




from  {{ ref('stg_erp_sales_invoice_header') }} as  a

left join {{ ref('stg_erp_inbound_sales_header') }} as  b  on a.no_ = b.documentno

left join {{ ref('stg_ofs_inboundsalesheader') }} as c on  c.weborderno = b.web_order_id

left join invoice_line as ii on ii.document_no_ = a.no_


--where web_order_id is null
  --where a.no_ in ('INV00427958', 'INV00427970')

 -- where web_order_id= 'O3070021S'
