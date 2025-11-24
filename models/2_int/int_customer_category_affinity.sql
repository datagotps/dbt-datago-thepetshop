-- models/intermediate/int_customer_category_affinity.sql
{{ config(
    materialized='table',
    description='Customer purchase patterns by product category, subcategory, and brand'
) }}

WITH category_purchases AS (
    SELECT 
        unified_customer_id,
        -- Updated Business Hierarchy
        item_division,       -- Level 1: Pet (DOG, CAT, FISH, etc.)
        item_block,          -- Level 2: Block (FOOD, ACCESSORIES, etc.)
        item_category,       -- Level 3: Category (Dry Food, Wet Food, etc.)
        item_subcategory,    -- Level 4: Subcategory (item type)
        item_brand,          -- Level 5: Brand

        -- Purchase Counts
        COUNT(DISTINCT unified_order_id) AS category_order_count,
        COUNT(DISTINCT document_no_) AS category_transaction_count,

        -- Revenue Metrics
        SUM(sales_amount__actual_) AS category_total_spend,
        AVG(sales_amount__actual_) AS category_avg_spend,

        -- Recency
        MAX(posting_date) AS category_last_purchase_date,
        MIN(posting_date) AS category_first_purchase_date,
        DATE_DIFF(CURRENT_DATE(), MAX(posting_date), DAY) AS category_days_since_last_purchase,

        -- Purchase Pattern
        COUNT(DISTINCT DATE_TRUNC(posting_date, MONTH)) AS category_active_months
    FROM {{ ref('int_order_lines') }}
    WHERE transaction_type = 'Sale'
      AND unified_customer_id IS NOT NULL
    GROUP BY 1,2,3,4,5,6  -- Added 6 for item_brand
),

customer_category_metrics AS (
    SELECT 
        unified_customer_id,
        COUNT(DISTINCT item_category)    AS total_categories_purchased,
        COUNT(DISTINCT item_subcategory) AS total_subcategories_purchased,
        COUNT(DISTINCT item_brand)       AS total_brands_purchased,
        ARRAY_AGG(
            STRUCT(item_category, category_total_spend, category_order_count) 
            ORDER BY category_total_spend DESC 
            LIMIT 3
        ) AS top_3_categories,
        COUNT(DISTINCT item_category) / NULLIF(COUNT(*), 0) AS category_diversity_score
    FROM category_purchases
    GROUP BY 1
),

category_flags AS (
    SELECT 
        unified_customer_id,
        -- Food Categories (Level 3: Category - from item_category)
        MAX(CASE WHEN item_category LIKE '%Wet Food%'  THEN category_order_count ELSE 0 END) AS wet_food_order_count,
        MAX(CASE WHEN item_category LIKE '%Dry Food%'  THEN category_order_count ELSE 0 END) AS dry_food_order_count,
        MAX(CASE WHEN item_category LIKE '%Treat%'     THEN category_order_count ELSE 0 END) AS treat_order_count,
        MAX(CASE WHEN item_category LIKE '%Litter%'    THEN category_order_count ELSE 0 END) AS litter_order_count,
        MAX(CASE WHEN item_category LIKE '%Hay%'       THEN category_order_count ELSE 0 END) AS hay_order_count,

        -- Pet Types (Level 1: Pet/Division - from item_division)
        MAX(CASE WHEN item_division = 'DOG'        THEN 1 ELSE 0 END) AS has_dog_purchases,
        MAX(CASE WHEN item_division = 'CAT'        THEN 1 ELSE 0 END) AS has_cat_purchases,
        MAX(CASE WHEN item_division = 'BIRD'       THEN 1 ELSE 0 END) AS has_bird_purchases,
        MAX(CASE WHEN item_division = 'FISH'       THEN 1 ELSE 0 END) AS has_fish_purchases,
        MAX(CASE WHEN item_division = 'SMALL PET'  THEN 1 ELSE 0 END) AS has_small_pet_purchases,
        MAX(CASE WHEN item_division = 'REPTILE'    THEN 1 ELSE 0 END) AS has_reptile_purchases,

        -- Livestock & Fish Specific (Level 2: Block - from item_block)
        MAX(CASE WHEN item_block = 'LIVESTOCK'            THEN category_order_count ELSE 0 END) AS livestock_order_count,
        MAX(CASE WHEN item_category LIKE '%Freshwater%'   THEN category_order_count ELSE 0 END) AS freshwater_fish_order_count,
        MAX(CASE WHEN item_category LIKE '%Marine%'       THEN category_order_count ELSE 0 END) AS marine_fish_order_count
    FROM category_purchases
    GROUP BY 1
),

-- New Pet Parent Detection from base lines (keeps category_purchases grain intact)
new_pet_parent_flags AS (
    SELECT
        unified_customer_id,
        MAX(
            CASE 
                WHEN (
                    LOWER(item_name) LIKE '%kitten%' 
                    OR LOWER(item_name) LIKE '%puppy%'
                )
                AND posting_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
                THEN 1 ELSE 0 
            END
        ) AS is_new_pet_parent
    FROM {{ ref('int_order_lines') }}
    WHERE transaction_type = 'Sale'
      AND unified_customer_id IS NOT NULL
    GROUP BY 1
),

brand_tier_flags AS (
    SELECT 
        unified_customer_id,
        MAX(CASE 
            WHEN item_brand IN ('Royal Canin', 'Hills', 'Orijen', 'Acana', 'Wellness', 'Blue Buffalo')
            THEN category_order_count ELSE 0 
        END) AS premium_brand_order_count,
        MAX(CASE 
            WHEN item_brand IN ('Pedigree', 'Whiskas', 'Purina', 'Friskies')
            THEN category_order_count ELSE 0 
        END) AS budget_brand_order_count
    FROM category_purchases
    GROUP BY 1
)

SELECT 
    cp.*,
    ccm.total_categories_purchased,
    ccm.total_subcategories_purchased,
    ccm.total_brands_purchased,
    ccm.category_diversity_score,
    cf.* EXCEPT(unified_customer_id),
    npf.is_new_pet_parent,
    btf.premium_brand_order_count,
    btf.budget_brand_order_count
FROM category_purchases cp
LEFT JOIN customer_category_metrics ccm USING (unified_customer_id)
LEFT JOIN category_flags cf USING (unified_customer_id)
LEFT JOIN new_pet_parent_flags npf USING (unified_customer_id)
LEFT JOIN brand_tier_flags btf USING (unified_customer_id)
