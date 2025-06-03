--recorder 25938372
select

a.*

--case when a.source_code != 'INVTADJMT' and a.sales_channel = 'Online' and a.document_type_2 != 'Sales Credit Memo' then b.amount else 0 end as online_line_amount_excl_vat,
--case when a.source_code != 'INVTADJMT' and a.sales_channel = 'Online' and a.document_type_2 != 'Sales Credit Memo' then b.amount_including_vat else 0 end as online_line_amount_enclu_vat,

--d.name,
--count(*)

from {{ ref('stg_erp_value_entry') }} as a
left join {{ ref('stg_erp_sales_invoice_line') }} as b on a.document_no_ = b.document_no_ and source_code = 'SALES' and a.document_line_no_ = b.line_no_
left join  {{ ref('stg_erp_inbound_sales_header') }}  as c  on a.document_no_ = c.documentno

left join {{ ref('stg_petshop_dimension_value') }} as d on  a.global_dimension_2_code = d.code
left join  {{ ref('int_items') }} as e on  e.item_no_ = a.item_no_