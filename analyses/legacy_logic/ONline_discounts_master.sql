select
    a.documentno,
    a.item_no_ as itemno,
    sum(
        a.shipping_charges - (a.shipping_charges * 0.05)
    ) as single_sku_shipping_charges,
    (
        sum(a.shipping_charges - (a.shipping_charges * 0.05)) / count(item_no_)
    ) as multiple_sku_shipping_charges,
    count(item_no_) as items,
    case
        when count(item_no_) > 1
        then ((sum(discount_amount) + sum(coupon_discount)) / count(item_no_)) * (-1)
        else (sum(discount_amount) + sum(coupon_discount)) * (-1)
    end as totaldiscount
from
    `sql_erp_prod_dbo.petshop_inbound_sales_line_c91094c2_db03_49d2_8cb9_95c179ecbf9d` a
where a.invoice_date >= '2021-06-01'
group by a.documentno, a.item_no_
