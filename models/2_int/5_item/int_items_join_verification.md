# int_items Vendor Join Verification

## Join Details

**Location:** `models/2_int/5_item/int_items.sql` (lines 53-54)

**Join Statement:**
```sql
LEFT JOIN {{ ref('stg_petshop_vendor') }} AS v ON v.no_ = a.VendorNo
```

---

## Join Analysis

### 1. Join Type: LEFT JOIN ✅
- **Type:** `LEFT JOIN` (preserves all records from left table)
- **Safety:** ✅ Safe - won't create duplicates
- **Behavior:** All items from `stg_petshop_item` are preserved, even if vendor doesn't exist

### 2. Join Key Analysis

**Left Table:** `stg_petshop_item` (aliased as `a`)
- **Join Key:** `a.VendorNo` (from `stg_petshop_item.vendor_no_`)
- **Uniqueness:** One `VendorNo` per item (item master has single vendor)

**Right Table:** `stg_petshop_vendor` (aliased as `v`)
- **Join Key:** `v.no_` (vendor number/ID)
- **Expected Uniqueness:** Should be unique (one vendor per vendor_no)

### 3. Potential Issues to Check

#### Issue 1: Duplicate Vendors in `stg_petshop_vendor`
**Risk:** If `stg_petshop_vendor.no_` has duplicates, LEFT JOIN could create multiple rows per item

**Mitigation:** 
- `stg_petshop_vendor` should have unique `no_` (vendor master table)
- Need to verify uniqueness

#### Issue 2: Multiple VendorNo per Item
**Risk:** If `stg_petshop_item` has duplicate items with different VendorNo

**Mitigation:**
- `stg_petshop_item` should have unique `item_no_`
- Each item has ONE `VendorNo` (primary vendor)
- Already verified: `int_items` has 61,511 records = 61,511 distinct items ✅

---

## Verification Queries

### Query 1: Check Vendor Uniqueness
```sql
SELECT 
    COUNT(*) as total_vendors,
    COUNT(DISTINCT no_) as distinct_vendor_nos,
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT no_) THEN 'UNIQUE - Safe'
        ELSE 'WARNING - Duplicates found'
    END as status
FROM stg_petshop_vendor
```

### Query 2: Check Item-Vendor Relationship
```sql
SELECT 
    COUNT(*) as total_items,
    COUNT(DISTINCT item_no_) as distinct_items,
    COUNT(DISTINCT VendorNo) as distinct_vendors,
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT item_no_) THEN 'UNIQUE - One record per item'
        ELSE 'WARNING - Duplicate items'
    END as status
FROM stg_petshop_item
```

### Query 3: Verify Join Doesn't Create Duplicates
```sql
WITH before_join AS (
    SELECT COUNT(*) as count_before
    FROM stg_petshop_item
),
after_join AS (
    SELECT COUNT(*) as count_after
    FROM stg_petshop_item AS a
    LEFT JOIN stg_petshop_vendor AS v ON v.no_ = a.VendorNo
)
SELECT 
    before_join.count_before,
    after_join.count_after,
    after_join.count_after - before_join.count_before as difference,
    CASE 
        WHEN before_join.count_before = after_join.count_after 
        THEN 'SAFE - No duplicates'
        ELSE 'WARNING - Duplicates created'
    END as status
FROM before_join, after_join
```

### Query 4: Check for Items with Multiple Vendor Matches
```sql
SELECT 
    a.item_no_,
    COUNT(*) as match_count,
    COUNT(DISTINCT v.no_) as distinct_vendors_matched
FROM stg_petshop_item AS a
LEFT JOIN stg_petshop_vendor AS v ON v.no_ = a.VendorNo
GROUP BY a.item_no_
HAVING COUNT(*) > 1
```

---

## Expected Results

### ✅ Safe Join Conditions:

1. **`stg_petshop_vendor.no_` is UNIQUE**
   - Vendor master table should have one record per vendor
   - Expected: `COUNT(*) = COUNT(DISTINCT no_)`

2. **`stg_petshop_item.item_no_` is UNIQUE**
   - Item master table should have one record per item
   - Already verified: ✅ 61,511 items = 61,511 distinct items

3. **One VendorNo per Item**
   - Each item has ONE primary vendor
   - Expected: No duplicate items with different vendors

4. **LEFT JOIN Behavior**
   - Preserves all items (even if vendor doesn't exist)
   - Won't create duplicates if vendor table is unique

---

## Current Verification Status

### ✅ Verified:
- **`int_items` uniqueness:** 61,511 records = 61,511 distinct items ✅
- **Join type:** LEFT JOIN ✅ (safe - preserves all items)
- **Join key:** `v.no_ = a.VendorNo` ✅ (correct)
- **Item uniqueness:** `stg_petshop_item` has unique `item_no_` ✅

### ⚠️ Potential Risk:
- **`stg_petshop_vendor` structure:** Joins two sources (`source_1` and `source_2`) on `no_`
- **Risk:** If `source_2` has multiple records per `no_`, LEFT JOIN could create duplicates
- **Mitigation:** Vendor master tables typically have unique vendor IDs

### ⚠️ Needs Verification (when tables are built):
- `stg_petshop_vendor.no_` uniqueness (check if source_2 has duplicates)
- Record count before/after vendor join
- Items with multiple vendor matches

---

## Join Safety Analysis

### Why Join Should Be Safe:

1. **LEFT JOIN Behavior:**
   - Preserves ALL items from `stg_petshop_item`
   - Only adds vendor attributes
   - Won't create duplicates if vendor table is unique

2. **Item Master Structure:**
   - Each item has ONE `VendorNo` (primary vendor)
   - `stg_petshop_item` has unique `item_no_`
   - One-to-one relationship: Item → Vendor

3. **Vendor Master Structure:**
   - Should have unique `no_` (vendor ID)
   - Master data table (one vendor per vendor_no)
   - **BUT:** Need to verify `source_2` doesn't have duplicates

4. **Current Output Verification:**
   - `int_items` has 61,511 records = 61,511 distinct items ✅
   - This proves the join doesn't create duplicates in final output

---

## Conclusion

**Join is SAFE** because:
1. ✅ Uses LEFT JOIN (preserves all items)
2. ✅ `int_items` output is unique (61,511 items = 61,511 records) - **PROVEN**
3. ✅ Join key is correct (`vendor_no_` to `vendor.no_`)
4. ✅ Item master has unique items
5. ⚠️ Vendor master should have unique vendors (needs verification)

**Recommendation:** 
- ✅ **Current join is safe** - output proves no duplicates
- Run verification queries in `verify_vendor_join.sql` when tables are built to confirm vendor uniqueness
- If vendor table has duplicates, consider adding DISTINCT or GROUP BY in vendor staging model

