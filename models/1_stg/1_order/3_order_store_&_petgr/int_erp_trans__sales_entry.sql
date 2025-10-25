select
*,
case 
when store_no_ = 'MOBILE' then 'Mobile Grooming'
when store_no_ = 'GRM' then 'Shop Grooming'
else null
end as clc_store_no_,

--f.description as item_subcategory,  --retail_product_code

FROM {{ ref('stg_erp_trans__sales_entry') }} as a
---left join {{ ref('stg_erp_retail_product_group') }} as f on f.code = a.retail_product_code

WHERE retail_product_code  IN ('31024','31010','31011','31012','31113','31114')

--and document_no_ = 'WSL-WT02-11195'