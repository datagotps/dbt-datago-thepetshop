select

*,
case when is_cpr = 1 then invoice_value_excl__tax_excl_ship else 0 end as rec_rev_in_period,
case when is_cpr = 0 then invoice_value_excl__tax_excl_ship else 0 end as rec_rev_deferred,

from  {{ ref('int_erp_occ_invoice_items') }} 