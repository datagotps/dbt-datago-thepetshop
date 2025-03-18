select

a.posting_date,
a.sales_amount__actual_,
a.location_code,




b.name,

a.document_no_,

a.item_no_,

a.inventory_posting_group,
a.source_posting_group,
a.source_code,


c.item_name,
c.item_category,
c.item_subcategory,

from {{ ref('stg_erp_value_entry') }} as a
left join {{ ref('stg_petshop_dimension_value') }} as b on  a.global_dimension_2_code = b.code

left join {{ ref('int_items') }} as c on  c.item_no_ = a.item_no_

where a.item_ledger_entry_type = 'Sale' and a.global_dimension_2_code  = 'POS Sale' and a.source_code = 'BACKOFFICE'


--DATE(a.posting_date) BETWEEN '2025-01-01' AND '2025-01-31'

