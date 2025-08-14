
select

c.description  AS division,
b.description AS item_category,
Case
When a.retail_product_code = '21035' Then 'Supplements'
else D.name
end AS item_subcategory,

a.item_brand,

a.item_name,

a.item_no_,
a.inventory_posting_group,









from  {{ ref('stg_petshop_item') }} as a
left join {{ ref('stg_petshop_item_category') }}  b ON a.item_category_code = b.code
left join {{ ref('stg_dimension_value') }} as d ON a.retail_product_code = d.code and d.dimension_code = 'PRODUCT GROUP'

left join {{ ref('stg_petshop_division') }} as c ON c.code = a.division_code


