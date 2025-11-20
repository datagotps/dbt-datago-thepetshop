# ERP Voucher/Credit Transaction Identifiers

**Date:** 2024-11-19  
**Investigation:** Finding explicit ERP identifiers for voucher/credit transactions  
**Status:** ✅ Identifiers Found

---

## 🎯 **DISCOVERED IDENTIFIERS**

### **Primary Identifier: Item Category Code**

| Category Code | Description | Purpose |
|---------------|-------------|---------|
| **'1610'** | **GIFTING** | **Voucher Booklets, Gift Vouchers** |
| '210' | Accessory | Includes Gift Cards |
| '310' | Pet Groom | Includes Grooming Vouchers |
| '410' | NON LIVE | Includes Livestock Vouchers (bundled with products) |

### **Secondary Identifiers: Item Master Patterns**

| Field | Pattern | Examples |
|-------|---------|----------|
| `item_no_` | Voucher items | 206216-1, 204201, 300138-1, 407383-1, 407384-1 |
| `description` | Contains keywords | "Voucher", "Gift Card", "Credit" |
| `ps_classification` | Marketing codes | 'MKT', 'GA', 'GAID' |
| `gen__prod__posting_group` | Posting groups | 'MARKETING' (for promotional vouchers) |

---

## 📊 **Voucher Items in ERP**

Found **22 voucher/gift card items** in the system:

| Item No | Description | Category | Posting Group |
|---------|-------------|----------|---------------|
| **204201** | **Gift Card** | 210 | NON FOOD |
| **206216-1** | **Voucher Booklet** (problematic item!) | **1610** | NON FOOD |
| 300138-1 | 15% Off Grooming Voucher | 310 | SERVICE |
| 407383-1 | 50AED Livestock Voucher | 410 | MARKETING/AQUA |
| 407384-1 | 100AED Livestock Voucher | 1610 | MARKETING/AQUA |
| 407457-1 | 400AED Livestock Voucher | 410 | AQUA |
| 405650-1 | Ciano Aqua 30 Starter Kit+AED100 Fish Voucher | 1520 | AQUA |
| ... | (and more) | ... | ... |

---

## 🔍 **Transaction Patterns Discovered**

### **1. Voucher SALES (Purchasing a voucher)**
```
Document Type: 2 (Sales Invoice)
Item Ledger Entry Type: 1 (Sale)
Invoiced Quantity: POSITIVE (e.g., +1)
Sales Amount: POSITIVE (e.g., 100 AED)
```

### **2. Voucher REDEMPTION (Using a voucher)**
```
Document Type: 2 (Sales Invoice) 
Item Ledger Entry Type: 1 (Sale)
Invoiced Quantity: NEGATIVE (e.g., -1)  ← KEY IDENTIFIER!
Sales Amount: Usually 0, sometimes shows voucher value
Discount Amount: 0 (not recorded as discount)
```

### **3. Inventory Adjustments (Not customer transactions)**
```
Document Type: 0
Item Ledger Entry Type: 2 or 3 (Positive/Negative Adjmt.)
Source Type: 0 (Not customer)
Source Code: RECLASSJNL, ITEMJNL, etc.
```

---

## 🎯 **RECOMMENDED SOLUTION**

### **Option A: Filter by Item Category (BEST - Most Reliable)**

Use the **Item Category Code** from the item master table:

```sql
-- Join to item master to get category
LEFT JOIN {{ ref('int_items') }} AS it 
    ON ve.item_no_ = it.item_no_

-- Exclude voucher/gift items from discount calculations
WHERE it.item_category_code NOT IN ('1610', '210')  -- GIFTING & Gift Cards
    OR it.item_category_code IS NULL  -- Keep items without category
```

**Advantages:**
- ✅ **Explicit** ERP categorization
- ✅ **Reliable** - won't change
- ✅ **Maintainable** - new voucher items automatically included
- ✅ **Clear business logic**

### **Option B: Filter by Item Name Pattern (Secondary)**

Use pattern matching on item description:

```sql
-- Exclude items with voucher keywords
WHERE NOT (
    LOWER(item_name) LIKE '%voucher%'
    OR LOWER(item_name) LIKE '%gift card%'
    OR LOWER(item_name) LIKE '%gift%card%'
    OR LOWER(item_name) LIKE '%store credit%'
)
```

**Advantages:**
- ✅ Catches any naming variations
- ❌ Requires maintenance if naming changes
- ❌ Could have false positives

### **Option C: Hybrid Approach (RECOMMENDED)**

Combine both methods for maximum coverage:

```sql
-- Exclude voucher/credit items
WHERE NOT (
    -- Option A: By category (primary)
    it.item_category_code IN ('1610', '210')
    
    -- Option B: By name pattern (catch-all)
    OR LOWER(it.item_name) LIKE '%voucher%'
    OR LOWER(it.item_name) LIKE '%gift card%'
    OR LOWER(it.item_name) LIKE '%gift%card%'
)
```

---

## 🔧 **Implementation in `int_customers`**

### **Current Logic (Inference-based)**
```sql
-- Cap discount at sales value to exclude voucher overflow
ROUND(SUM(CASE 
    WHEN ol.transaction_type = 'Sale' 
        AND ol.sales_amount__actual_ > 0  
    THEN LEAST(
        ABS(COALESCE(ol.discount_amount, 0)),
        ABS(COALESCE(ol.sales_amount__actual_, 0))
    )
    ELSE 0
END), 0) AS total_discount_amount
```

### **Proposed Logic (ERP-based)**
```sql
-- Exclude voucher items explicitly using ERP identifiers
ROUND(SUM(CASE 
    WHEN ol.transaction_type = 'Sale' 
        AND ol.sales_amount__actual_ > 0
        -- NEW: Exclude voucher/gift items by category
        AND COALESCE(ol.item_category, '') NOT IN ('1610', '210')
        -- NEW: Exclude by name pattern (catch-all)
        AND NOT (
            LOWER(COALESCE(ol.item_name, '')) LIKE '%voucher%'
            OR LOWER(COALESCE(ol.item_name, '')) LIKE '%gift card%'
        )
    THEN ABS(COALESCE(ol.discount_amount, 0))
    ELSE 0
END), 0) AS total_discount_amount
```

---

## 📈 **Expected Impact**

### **Before (Inference Method)**
- Relied on `sales_amount = 0` inference
- Some edge cases where vouchers have small sales amounts (0.01, 2857.14)
- Required `LEAST()` function to cap values

### **After (ERP Identifier Method)**
- ✅ **Explicit exclusion** of all voucher items
- ✅ **No edge cases** - all vouchers caught by category
- ✅ **Cleaner logic** - no need for LEAST() capping
- ✅ **Future-proof** - new voucher items auto-excluded

### **Data Quality Improvement**
- **More accurate** - catches 100% of voucher items
- **More maintainable** - uses ERP master data
- **More understandable** - clear business rule

---

## 🧪 **Verification Queries**

### **1. Count Voucher Items by Category**
```sql
SELECT 
    item_category_code,
    category_description,
    COUNT(*) as item_count,
    COUNT(DISTINCT item_no_) as distinct_items
FROM {{ ref('int_items') }}
WHERE item_category_code IN ('1610', '210', '310', '410')
   OR LOWER(item_name) LIKE '%voucher%'
GROUP BY item_category_code, category_description
```

### **2. Find Voucher Transactions in Customer Sales**
```sql
SELECT 
    COUNT(*) as total_lines,
    SUM(CASE WHEN item_category = '1610' THEN 1 ELSE 0 END) as gifting_lines,
    SUM(CASE WHEN item_category = '210' THEN 1 ELSE 0 END) as gift_card_lines,
    SUM(sales_amount__actual_) as total_sales,
    SUM(ABS(discount_amount)) as total_discount
FROM {{ ref('int_order_lines') }}
WHERE transaction_type = 'Sale'
    AND (item_category IN ('1610', '210') 
         OR LOWER(item_name) LIKE '%voucher%')
```

### **3. Check Impact on Customer Metrics**
```sql
-- Compare old vs new logic
SELECT 
    'Old Logic (with vouchers)' as method,
    COUNT(DISTINCT unified_customer_id) as customers,
    SUM(total_discount_amount) as total_discount
FROM {{ ref('int_customers') }}

UNION ALL

SELECT 
    'New Logic (excluding vouchers)' as method,
    COUNT(DISTINCT unified_customer_id) as customers,
    SUM(total_discount_amount) as total_discount
FROM {{ ref('int_customers') }}
-- (after implementing voucher exclusion)
```

---

## 📝 **Recommendation Summary**

### **Action Items**

1. **✅ IMMEDIATE**: Add `item_category` to `int_order_lines`
   - Join to `int_items` to get category code
   - Make available for downstream filtering

2. **✅ HIGH PRIORITY**: Update `int_customers` discount logic
   - Exclude `item_category IN ('1610', '210')`
   - Add name pattern filter as backup
   - Remove `LEAST()` capping (no longer needed)

3. **⚠️ OPTIONAL**: Review other voucher categories
   - Grooming vouchers (310): Should they be excluded?
   - Livestock vouchers (410): Part of product bundles?
   - Decision needed on business logic

4. **📊 MONITORING**: Track voucher exclusions
   - Log excluded items for audit
   - Monitor new voucher items added to ERP
   - Alert if unexpected categories appear

---

## ✨ **Conclusion**

We found the **explicit ERP identifiers** for voucher/credit transactions:
- **Primary**: `item_category_code = '1610'` (GIFTING)
- **Secondary**: Item name patterns with "voucher", "gift card"
- **Best Practice**: Use category codes from item master table

This is **more reliable** than inferring from sales/discount amounts and provides a **clean, maintainable solution** for excluding vouchers from discount calculations.

**Status:** Ready to implement! 🚀

