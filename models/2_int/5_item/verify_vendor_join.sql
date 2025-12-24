-- =====================================================
-- Verification Script: Vendor Join Safety Check
-- Purpose: Verify that vendor join doesn't create duplicates
-- Location: int_items.sql line 53-54
-- =====================================================

-- Check 1: Verify stg_petshop_vendor.no_ uniqueness
SELECT 
    'Check 1: Vendor Uniqueness' as check_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT no_) as distinct_vendor_nos,
    COUNT(*) - COUNT(DISTINCT no_) as potential_duplicates,
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT no_) THEN '✅ PASS - Unique vendor_no_'
        ELSE '❌ FAIL - Duplicate vendor_no_ found'
    END as status
FROM {{ ref('stg_petshop_vendor') }};

-- Check 2: Verify stg_petshop_item uniqueness
SELECT 
    'Check 2: Item Uniqueness' as check_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT item_no_) as distinct_items,
    COUNT(*) - COUNT(DISTINCT item_no_) as potential_duplicates,
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT item_no_) THEN '✅ PASS - Unique items'
        ELSE '❌ FAIL - Duplicate items found'
    END as status
FROM {{ ref('stg_petshop_item') }};

-- Check 3: Verify join doesn't create duplicates
WITH before_join AS (
    SELECT COUNT(*) as count_before
    FROM {{ ref('stg_petshop_item') }}
),
after_join AS (
    SELECT COUNT(*) as count_after
    FROM {{ ref('stg_petshop_item') }} AS a
    LEFT JOIN {{ ref('stg_petshop_vendor') }} AS v ON v.no_ = a.VendorNo
)
SELECT 
    'Check 3: Join Record Count' as check_name,
    before_join.count_before as records_before_join,
    after_join.count_after as records_after_join,
    after_join.count_after - before_join.count_before as difference,
    CASE 
        WHEN before_join.count_before = after_join.count_after 
        THEN '✅ PASS - No duplicates created'
        ELSE '❌ FAIL - Record count changed'
    END as status
FROM before_join, after_join;

-- Check 4: Find items with multiple vendor matches (should be 0)
SELECT 
    'Check 4: Items with Multiple Vendor Matches' as check_name,
    a.item_no_,
    COUNT(*) as match_count,
    COUNT(DISTINCT v.no_) as distinct_vendors_matched,
    STRING_AGG(DISTINCT CAST(a.VendorNo AS STRING), ', ') as vendor_nos,
    STRING_AGG(DISTINCT v.name, ', ') as vendor_names
FROM {{ ref('stg_petshop_item') }} AS a
LEFT JOIN {{ ref('stg_petshop_vendor') }} AS v ON v.no_ = a.VendorNo
GROUP BY a.item_no_
HAVING COUNT(*) > 1
LIMIT 10;

-- Check 5: Verify int_items final output uniqueness
SELECT 
    'Check 5: int_items Output Uniqueness' as check_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT item_no_) as distinct_items,
    COUNT(*) - COUNT(DISTINCT item_no_) as potential_duplicates,
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT item_no_) THEN '✅ PASS - Unique output'
        ELSE '❌ FAIL - Duplicate items in output'
    END as status
FROM {{ ref('int_items') }};

