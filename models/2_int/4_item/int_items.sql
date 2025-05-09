
select

a.item_no_,
a.item_name,
a.division,
a.item_brand,


b.description AS item_category,

Case
When a.retail_product_code = '21035' Then 'Supplements'
else D.name
end AS item_subcategory,

from  {{ ref('stg_petshop_item') }} as a
left join {{ ref('stg_petshop_item_category') }}  b ON a.item_category_code = b.code
left join {{ ref('stg_petshop_dimension_value') }} as d ON a.retail_product_code = d.code
