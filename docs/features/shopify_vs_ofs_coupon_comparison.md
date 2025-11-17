# Shopify vs OFS Coupon Code Comparison Analysis

## Test Case: Order O3083424S (INV00441924)
**Date:** Feb 18, 2025  
**Customer:** Omayya Ismail

---

## 🔍 Data Comparison

### 1️⃣ Current dbt Model (fact_commercial)
**Source:** `stg_ofs_inboundpaymentline` → `int_value_entry` → `fact_commercial`

```
document_no_: INV00441924
web_order_id: O3083424S
posting_date: 2025-02-18
online_discount_amount: -6.92 AED (single line item shown)
online_offer_no_: "" (EMPTY)
```

### 2️⃣ OFS Payment Line Table (stg_ofs_inboundpaymentline)
**Direct Source Check:**

```sql
SELECT weborderno, itemid, couponcode, discount
FROM stg_ofs_inboundpaymentline
WHERE weborderno = 'O3083424S'
```

**Result:**
- **15 line items** in the order
- **couponcode: "" (EMPTY)** for ALL items
- Total discount in OFS: 162.64 AED (sum of all line discounts)

| Item ID | Amount | Discount | Coupon Code |
|---------|--------|----------|-------------|
| 5447240 | 10.33 | 1.91 | "" |
| 5447241 | 8.51 | 1.57 | "" |
| 5447242 | 8.50 | 1.57 | "" |
| 5447243 | 44.45 | 8.23 | "" |
| 5447244 | 39.27 | 7.27 | "" |
| ... | ... | ... | "" |
| **Total** | **901.90** | **162.64** | **EMPTY** |

### 3️⃣ Shopify Orders Table (Native Shopify Data)
**Source:** `tps-data-386515.shopify.orders`

```sql
SELECT name, total_price, total_discounts, discount_codes
FROM shopify.orders
WHERE name = 'O3083424S'
```

**Result:**
```json
{
  "order_number": "O3083424S",
  "created_at": "2025-02-18T11:54:25Z",
  "total_price": 1051.86,
  "total_discounts": 183.78,
  "discount_codes": [
    {
      "code": "PET15",
      "amount": 183.78,
      "type": "percentage"
    }
  ]
}
```

✅ **Shopify HAS the coupon code: "PET15"**

---

## 🚨 Critical Finding

### **The Coupon Code EXISTS in Shopify but NOT in OFS!**

| Data Source | Coupon Code | Discount Amount | Status |
|-------------|-------------|-----------------|--------|
| **Shopify** | **PET15** ✅ | 183.78 AED | **HAS CODE** |
| **OFS Payment Line** | "" ❌ | 162.64 AED | **NO CODE** |
| **fact_commercial** | "" ❌ | 6.92 AED (partial) | **NO CODE** |

---

## 💡 Root Cause Analysis

### Why is the coupon code missing in OFS?

**Hypothesis 1: Data Sync Issue**
- Shopify captures the coupon code at checkout
- OFS (Order Fulfillment System) may not receive or store the coupon code
- The discount amount flows through, but the coupon identifier is lost

**Hypothesis 2: System Architecture**
- OFS may calculate discounts independently based on product prices
- Coupon code is a Shopify-specific field not mapped to OFS schema
- OFS focuses on fulfillment, not marketing attribution

**Hypothesis 3: Timing/Integration Gap**
- Coupon code might be in a different OFS table not currently joined
- Could be in header-level data (isheader=1) instead of line-level

---

## 📊 Value Assessment: Should We Bring Shopify Data?

### ✅ **YES - High Value Benefits:**

1. **Complete Coupon Code Coverage**
   - Shopify has ALL coupon codes for online orders
   - OFS is missing this critical marketing attribution field
   - Current model has blind spots for campaign tracking

2. **Better Campaign Attribution**
   - Can link orders to specific marketing campaigns
   - Track coupon performance (PET15, EKTPS2024, CMENA, etc.)
   - Measure ROI of promotional codes

3. **Reconciliation & Data Quality**
   - Cross-validate discount amounts between systems
   - Identify discrepancies (183.78 in Shopify vs 162.64 in OFS)
   - Improve data accuracy

4. **Marketing Analytics**
   - Customer acquisition source (which coupon brought them)
   - Coupon usage patterns
   - A/B testing of different codes

### ⚠️ **Considerations:**

1. **Discount Amount Mismatch**
   - Shopify: 183.78 AED
   - OFS: 162.64 AED
   - Need to understand which is correct (likely VAT difference)

2. **Data Integration Effort**
   - Need to join Shopify orders to fact_commercial
   - Join key: `web_order_id` = Shopify `name`
   - Additional ETL/sync process required

3. **Thematic Campaigns Still Not Solved**
   - Shopify only tracks code-based discounts
   - Price-off campaigns still invisible (as per meeting notes)
   - This solves code attribution, not price-off attribution

---

## 🎯 Recommendation

### **Bring Shopify Orders Data to the Model**

**Implementation:**

```sql
-- Proposed enhancement to int_value_entry or fact_commercial
LEFT JOIN `tps-data-386515.shopify.orders` AS shopify
    ON ve.web_order_id = shopify.name

-- Extract coupon code
COALESCE(
    isl.couponcode,  -- Current OFS source (often empty)
    shopify.discount_codes[0].code  -- Shopify source (reliable)
) AS online_offer_no_
```

**Benefits:**
- ✅ Fill gaps where OFS couponcode is empty
- ✅ Enable campaign performance tracking
- ✅ Support marketing team's needs (per meeting notes)
- ✅ Align with Sachin's manual process (he uses Shopify exports)

**Priority:** **HIGH**
- Directly addresses the corner case identified
- Enables automated campaign reporting (5 hours/week savings)
- Required for Campaign Summary dashboard (per meeting action items)

---

## 📋 Action Items

| Owner | Action | Priority | Status |
|-------|--------|----------|--------|
| Data Team | Set up Shopify API sync to BigQuery | HIGH | Pending |
| Anmar | Add Shopify orders join to int_value_entry | HIGH | Pending |
| Anmar | Create COALESCE logic for coupon code (OFS → Shopify fallback) | HIGH | Pending |
| Data Team | Investigate discount amount discrepancy (183.78 vs 162.64) | MEDIUM | Pending |
| Anmar | Document Shopify as primary source for online coupon codes | MEDIUM | Pending |
| Anmar | Update email to team with this finding | HIGH | Pending |

---

## 🔗 Related Documents

- `commercial_discount_analysis_20251025.md` - Current discount model documentation
- `email_discount_analysis_clarification.md` - Email draft to team
- `Meeting_Sachin_Campaign_Performance_20250626_20251024.md` - Meeting notes on campaign tracking requirements

---

## Test Orders for Validation

| Order Number | Date | Shopify Coupon | OFS Coupon | Use Case |
|--------------|------|----------------|------------|----------|
| O3083424S | 2025-02-18 | **PET15** | Empty | Gap example |
| O3083200S | 2025-02-17 | **EKTPS2024** | Empty | Gap example |
| O3083106S | 2025-02-17 | **EKTPS2024** | Empty | Gap example |
| INV00537669 | 2025-09-30 | (Not in Shopify sample) | Empty | Price-off example |

---

**Conclusion:** Shopify orders table provides critical coupon code data that is missing from OFS. Integration is essential for complete online discount analysis and campaign attribution.
