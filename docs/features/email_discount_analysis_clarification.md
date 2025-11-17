# Email: Clarification Needed on Discount Analysis Data Flow

---

**Subject:** Clarification Needed: Online & Offline Discount Data Flow and Offer Types

**To:** [Business/Operations Team]  
**CC:** [Data Team]  
**Priority:** Normal

---

## Context: Discount Analysis Feature

We have implemented a comprehensive discount analysis feature in our data warehouse that tracks promotional discounts across all sales channels. The feature captures:

### Current Data Sources:

**Offline Discounts (Shop Channel):**
- Source: `stg_value_entry.discount_amount` + `int_discount_ledger_entry`
- Offer Details: `offer_no_` and `offline_offer_name` from `stg_periodic_discount`
- Example: Order DIP-DT01-129795 with offer "MGR15" (15% manager discount)

**Online Discounts (Website/App):**
- Source: `stg_erp_inbound_sales_line.discount_amount` + `stg_ofs_inboundpaymentline.couponcode`
- Offer Details: `couponcode` field (when available)
- VAT Adjustment: Online discounts are calculated VAT-exclusive (÷ 1.05)

---

## Issue Identified: Inconsistent Online Offer Data

We've discovered a **corner case** in online discount tracking where the `online_offer_no_` (coupon code) field is **sometimes populated and sometimes empty**, even when discounts are applied.

### Examples:

| Order Number | Discount Amount | Coupon Code | Status |
|--------------|-----------------|-------------|--------|
| **INV00537669** | -766.28 AED (20%) | **Empty** | ❌ No coupon code captured |
| **INV00519852** | -3.43 AED (20%) | "Dog Wet Food - Buy 5 & GET 20% off" | ✅ Coupon code captured |

### Data Observation:
```sql
-- Order INV00537669: Has discount but NO coupon code
discount_amount: -766.28 AED
online_offer_no_: "" (empty)

-- Order INV00519852: Has discount AND coupon code
discount_amount: -3.43 AED
online_offer_no_: "Dog Wet Food - Buy 5 & GET 20% off"
```

---

## Questions for the Team

### 1. **Multiple Discount/Offer Processes**
Are there multiple types of discount mechanisms in our online channel?
- Coupon-based discounts (customer enters code)
- Automatic promotional discounts (site-wide sales, category discounts)
- Flash sales or time-based promotions
- Loyalty program discounts
- Bundle/quantity-based discounts (Buy X Get Y% off)

### 2. **Data Capture Requirements**
For discounts **without coupon codes**, is there another field or table where we can capture:
- Promotion name/ID?
- Discount type/category?
- Campaign reference?

### 3. **Business Process Documentation**
Could you provide or help us create documentation for:

**A. Online Discount Flow:**
- All possible discount types
- How each type is triggered
- Where each type is recorded in the system
- Expected data fields for each type

**B. Offline Discount Flow:**
- All possible discount types (manager discount, periodic offers, etc.)
- Approval process (if any)
- System recording mechanism

---

## Requested Deliverables

To ensure accurate discount analysis and reporting, we need:

1. **📋 Discount Type Matrix**
   - List of all discount types (online & offline)
   - How to identify each type in the data
   - Business rules for each type

2. **📊 Data Flow Diagram**
   - Source systems for each discount type
   - Tables and fields involved
   - Any transformations or calculations

3. **📝 Business Rules Document**
   - When discounts can be combined
   - Priority/hierarchy of discounts
   - Validation rules

---

## Impact on Reporting

Currently, our discount analysis can:
- ✅ Track total discount amounts (online & offline)
- ✅ Calculate discount percentages
- ✅ Identify discounted vs. non-discounted transactions
- ⚠️ **Limited ability to categorize online discount types** when coupon code is empty

With proper documentation, we can enhance reporting to:
- Segment customers by discount type usage
- Measure effectiveness of different promotion types
- Provide campaign-level ROI analysis
- Identify discount abuse patterns

---

## Next Steps

1. Please review the questions above
2. Schedule a 30-minute meeting to discuss discount processes
3. Share any existing documentation on discount/promotion workflows
4. Identify SMEs for online and offline discount processes

---

**Sample Data for Reference:**

**Online Order with Coupon:**
```
Order: INV00519852
Coupon: "Dog Wet Food - Buy 5 & GET 20% off"
Discount: 20% applied
```

**Online Order without Coupon:**
```
Order: INV00537669
Coupon: (empty)
Discount: 20% applied automatically
```

**Offline Order:**
```
Order: DIP-DT01-129795
Offer: MGR15 (Manager 15% discount)
Discount: 15% applied
```

---

Please let me know your availability for a discussion, or feel free to respond with any clarifications.

**Best regards,**  
[Your Name]  
Data Analytics Team

---

**Attachments:**
- Discount Analysis Feature Documentation: `docs/features/commercial_discount_analysis_20251025.md`
- Example Queries: `docs/features/example_online_discount_order_simple_query.sql`
