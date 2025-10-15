
select

c.description  AS division,
b.description AS item_category,    --item_category
f.description as item_subcategory,  --retail_product_code
e.description as item_type,   --item_subcategory




a.item_brand,

a.item_name,

a.item_no_,
a.inventory_posting_group,

    CASE 
        WHEN c.description = 'DOG' THEN 1
        WHEN c.description = 'CAT' THEN 2
        WHEN c.description = 'FISH' THEN 3
        WHEN c.description = 'SERVICE' THEN 4
        WHEN c.description = 'BIRD' THEN 5
        WHEN c.description = 'SMALL PET' THEN 6
        WHEN c.description = 'REPTILE' THEN 7
        WHEN c.description = 'HUMAN' THEN 8

        WHEN c.description = 'AQUA' THEN 9
        WHEN c.description = 'NON FOOD' THEN 10
        WHEN c.description = 'FOOD' THEN 11

        ELSE 999  -- For any unexpected values
    END AS division_sort_order,


    -- Add sort order column for item_category
    CASE 
        WHEN b.description = 'FOOD' THEN 1
        WHEN b.description = 'ACCESSORIES' THEN 2
        WHEN b.description = 'HEALTH & HYGIENE' THEN 3
        WHEN b.description = 'Pet Relocation' THEN 4
        WHEN b.description = 'LIVESTOCK' THEN 5
        WHEN b.description = 'GIFTING' THEN 6
        WHEN b.description = 'Pet Groom' THEN 7
        WHEN b.description = 'LIVE STOCK' THEN 8
        WHEN b.description = 'NON LIVE' THEN 9
        WHEN b.description = 'Accessory' THEN 10
        WHEN b.description = 'Other Food' THEN 11
        ELSE 999  -- For any unexpected values
    END AS item_category_sort_order,

/*
CASE
    WHEN item_subcategory = 'Dry Food' THEN 1
    WHEN item_subcategory = 'Wet Food' THEN 2
    WHEN item_subcategory = 'Treats' THEN 3
    WHEN item_subcategory = 'Litter & Bedding' THEN 4
    WHEN item_subcategory = 'Habitat' THEN 5
    WHEN item_subcategory = 'Cleaning & Potty' THEN 6
    WHEN item_subcategory = 'Toys' THEN 7
    WHEN item_subcategory = 'Supplements & Wellness' THEN 8
    WHEN item_subcategory = 'Walking' THEN 9
    WHEN item_subcategory = 'Scratching' THEN 10
    WHEN item_subcategory = 'Other Diets' THEN 11
    WHEN item_subcategory = 'Feeders & Waterers' THEN 12
    WHEN item_subcategory = 'Pet Relocation' THEN 13
    WHEN item_subcategory = 'Grooming' THEN 14
    WHEN item_subcategory = 'Tanks, Cabinets & Stands' THEN 15
    WHEN item_subcategory = 'Travel & Safety' THEN 16
    WHEN item_subcategory = 'Food For Bird' THEN 17
    WHEN item_subcategory = 'Food For Small Pet' THEN 18
    WHEN item_subcategory = 'Live Freshwater Fish' THEN 19
    WHEN item_subcategory = 'Tick & Flea' THEN 20
    WHEN item_subcategory = 'Equipment' THEN 21
    WHEN item_subcategory = 'Decoration' THEN 22
    WHEN item_subcategory = 'Lights, Pumps & Heaters' THEN 23
    WHEN item_subcategory = 'Food For Fish' THEN 24
    WHEN item_subcategory = 'Live Marine Fish' THEN 25
    WHEN item_subcategory = 'Clothing' THEN 26
    WHEN item_subcategory = 'Coral' THEN 27
    WHEN item_subcategory = 'Plants' THEN 28
    WHEN item_subcategory = 'Marine Invertebrates' THEN 29
    WHEN item_subcategory = 'Cleaning & Maintenance' THEN 30
    WHEN item_subcategory = 'Food For Reptile' THEN 31
    WHEN item_subcategory = 'Gifts & Home' THEN 32
    WHEN item_subcategory = 'Heating & Lightning' THEN 33
    WHEN item_subcategory = 'Substrate & Bedding' THEN 34
    WHEN item_subcategory = 'Freshwater Inverts' THEN 35
    WHEN item_subcategory = 'Dog Groom' THEN 36
    WHEN item_subcategory = 'PLANT Maintenance' THEN 37
    WHEN item_subcategory = 'Add-on' THEN 38
    WHEN item_subcategory = 'Marine Inverts' THEN 39

    WHEN item_subcategory = 'Horse Accessory' THEN 40
    WHEN item_subcategory = 'Fish Food' THEN 41
    ELSE 999
END AS item_subcategory_sort_order,

*/

a.varient_item,



from  {{ ref('stg_petshop_item') }} as a
left join {{ ref('stg_petshop_item_category') }}  b ON a.item_category_code = b.code
left join {{ ref('stg_petshop_division') }} as c ON c.code = a.division_code
left join {{ ref('stg_dimension_value') }} as d ON a.retail_product_code = d.code and d.dimension_code = 'PRODUCT GROUP'


left join {{ ref('stg_petshop_item_sub_category') }} as e ON e.code = a.item_sub_category

left join {{ ref('stg_erp_retail_product_group') }} as f on f.code = a.retail_product_code

--where a.item_no_ = '100001-1'