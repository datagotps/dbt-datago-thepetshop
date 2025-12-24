-- =====================================================
-- INT_ITEMS: Item Dimension with Dynamic Revenue-Based Sort Orders
-- Sort orders are calculated from actual sales revenue (highest revenue = 1)
-- Full 5-Level Product Hierarchy: Division > Block > Category > Subcategory > Brand
-- =====================================================

-- Step 1: Get revenue per item from value_entry (earliest source)
WITH item_revenue AS (
    SELECT 
        item_no_,
        SUM(CASE WHEN item_ledger_entry_type = 1 THEN sales_amount__actual_ ELSE 0 END) as total_revenue
    FROM {{ ref('stg_value_entry') }}
    WHERE item_no_ IS NOT NULL
    GROUP BY item_no_
),

-- Step 2: Base item data with hierarchy
base_items AS (
    SELECT
        a.item_no_,
        a.item_name,
        a.item_brand,
        a.inventory_posting_group,
        a.varient_item,
        -- Item purchase type
        a.item_purchase_type,
        -- Vendor information
        a.VendorNo AS vendor_no_,
        v.vendor_posting_group,
        v.vendor_purchase_type,
        v.name AS vendor_name,
        -- Brand Ownership Type Classification
        CASE 
            WHEN v.vendor_posting_group = 'Foreign' 
                 AND a.item_brand IN ('Regalia', 'Bud & Billy', 'ThunderPaws', 'Kitty Clean') 
            THEN 'Private Label'
            WHEN v.vendor_posting_group = 'Foreign' 
            THEN 'Own Brand'
            ELSE 'Other Brand'
        END AS brand_ownership_type,
        -- Hierarchy columns
        c.description AS item_division,      -- Level 1: Pet (from stg_petshop_division)
        b.description AS item_block,         -- Level 2: Block (from stg_petshop_item_category)
        f.description AS item_category,      -- Level 3: Category (from stg_erp_retail_product_group)
        e.description AS item_subcategory,   -- Level 4: Subcategory (from stg_petshop_item_sub_category)
        -- Revenue for this item
        COALESCE(ir.total_revenue, 0) AS item_revenue
    FROM {{ ref('stg_petshop_item') }} AS a
    LEFT JOIN {{ ref('stg_petshop_item_category') }} AS b ON a.item_category_code = b.code
    LEFT JOIN {{ ref('stg_petshop_division') }} AS c ON c.code = a.division_code
    LEFT JOIN {{ ref('stg_dimension_value') }} AS d ON a.retail_product_code = d.code AND d.dimension_code = 'PRODUCT GROUP'
    LEFT JOIN {{ ref('stg_petshop_item_sub_category') }} AS e ON e.code = a.item_sub_category
    LEFT JOIN {{ ref('stg_erp_retail_product_group') }} AS f ON f.code = a.retail_product_code
    LEFT JOIN {{ ref('stg_petshop_vendor') }} AS v ON v.no_ = a.VendorNo
    LEFT JOIN item_revenue AS ir ON ir.item_no_ = a.item_no_
),

-- Step 3: Calculate revenue per Division (Level 1)
division_revenue AS (
    SELECT 
        item_division,
        SUM(item_revenue) AS hierarchy_revenue
    FROM base_items
    WHERE item_division IS NOT NULL
    GROUP BY item_division
),

division_sort AS (
    SELECT 
        item_division,
        hierarchy_revenue,
        DENSE_RANK() OVER (ORDER BY hierarchy_revenue DESC) AS item_division_sort_order
    FROM division_revenue
),

-- Step 4: Calculate revenue per Block (Level 2)
block_revenue AS (
    SELECT 
        item_block,
        SUM(item_revenue) AS hierarchy_revenue
    FROM base_items
    WHERE item_block IS NOT NULL
    GROUP BY item_block
),

block_sort AS (
    SELECT 
        item_block,
        hierarchy_revenue,
        DENSE_RANK() OVER (ORDER BY hierarchy_revenue DESC) AS item_block_sort_order
    FROM block_revenue
),

-- Step 5: Calculate revenue per Category (Level 3)
category_revenue AS (
    SELECT 
        item_category,
        SUM(item_revenue) AS hierarchy_revenue
    FROM base_items
    WHERE item_category IS NOT NULL
    GROUP BY item_category
),

category_sort AS (
    SELECT 
        item_category,
        hierarchy_revenue,
        DENSE_RANK() OVER (ORDER BY hierarchy_revenue DESC) AS item_category_sort_order
    FROM category_revenue
),

-- Step 6: Calculate revenue per Subcategory (Level 4)
subcategory_revenue AS (
    SELECT 
        item_subcategory,
        SUM(item_revenue) AS hierarchy_revenue
    FROM base_items
    WHERE item_subcategory IS NOT NULL
    GROUP BY item_subcategory
),

subcategory_sort AS (
    SELECT 
        item_subcategory,
        hierarchy_revenue,
        DENSE_RANK() OVER (ORDER BY hierarchy_revenue DESC) AS item_subcategory_sort_order
    FROM subcategory_revenue
),

-- Step 7: Calculate revenue per Brand (Level 5)
brand_revenue AS (
    SELECT 
        item_brand,
        SUM(item_revenue) AS hierarchy_revenue
    FROM base_items
    WHERE item_brand IS NOT NULL
    GROUP BY item_brand
),

brand_sort AS (
    SELECT 
        item_brand,
        hierarchy_revenue,
        DENSE_RANK() OVER (ORDER BY hierarchy_revenue DESC) AS item_brand_sort_order
    FROM brand_revenue
)

-- Final output: Items with dynamic sort orders based on revenue contribution
SELECT
    -- Item Identifiers
    bi.item_no_,
    bi.item_name,
    bi.inventory_posting_group,
    bi.varient_item,
    
    -- Item Purchase Type
    bi.item_purchase_type,
    
    -- Vendor Information
    bi.vendor_no_,
    bi.vendor_posting_group,
    bi.vendor_purchase_type,
    bi.vendor_name,
    bi.brand_ownership_type,
    
    -- Full Product Hierarchy (5 Levels)
    bi.item_division,                                           -- Level 1: Pet
    bi.item_block,                                              -- Level 2: Block
    bi.item_category,                                           -- Level 3: Category
    bi.item_subcategory,                                        -- Level 4: Subcategory
    bi.item_brand,                                              -- Level 5: Brand
    
    -- Dynamic Sort Orders (based on revenue - highest revenue = 1)
    COALESCE(ds.item_division_sort_order, 999) AS item_division_sort_order,
    COALESCE(bs.item_block_sort_order, 999) AS item_block_sort_order,
    COALESCE(cs.item_category_sort_order, 999) AS item_category_sort_order,
    COALESCE(ss.item_subcategory_sort_order, 999) AS item_subcategory_sort_order,
    COALESCE(brs.item_brand_sort_order, 999) AS item_brand_sort_order

FROM base_items AS bi
LEFT JOIN division_sort AS ds ON ds.item_division = bi.item_division
LEFT JOIN block_sort AS bs ON bs.item_block = bi.item_block
LEFT JOIN category_sort AS cs ON cs.item_category = bi.item_category
LEFT JOIN subcategory_sort AS ss ON ss.item_subcategory = bi.item_subcategory
LEFT JOIN brand_sort AS brs ON brs.item_brand = bi.item_brand
