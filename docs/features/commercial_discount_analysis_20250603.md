# Feature Name: Commercial Discount Analysis (Order Line Level)

## 1. Business Purpose
Provides **granular discount and promotional tracking at the order line item level**, enabling detailed analysis of promotional effectiveness, margin impact, and revenue optimization across all sales channels.

**Why it was built**: To answer critical commercial and finance questions about promotional performance:
- What is the revenue impact of each promotion/coupon code?
- Which products have the highest discount rates?
- How do discounts affect gross margin by category/channel?
- What percentage of sales come from discounted vs. full-price transactions?
- Which promotional offers drive the most volume vs. revenue?
- How do online vs. offline discounts compare in redemption and effectiveness?

**Business Value**:
- **Promotional ROI Analysis**: Track revenue, margin, and volume impact of every promotion
- **Price Optimization**: Understand which products/categories require discounts to sell
- **Channel Comparison**: Compare discount effectiveness across online, offline, and affiliate channels
- **Margin Protection**: Identify excessive discounting that erodes profitability
- **Campaign Performance**: Measure success of marketing campaigns through coupon redemption
- **Inventory Management**: Track clearance discount effectiveness

**Dashboard Usage**: Powers commercial analytics dashboards, promotional performance reports, margin analysis, discount waterfall reports, and pricing strategy decision support.

## 2. Technical Overview (dbt)

**Models Created/Modified**:

*Online Order Data Stitching (OFS → ERP):*
- `stg_ofs_inboundpaymentline` - Online order discount data from OFS system
- `stg_erp_inbound_sales_line` - ERP sales line with discount amounts
- `int_value_entry` - **Stitching layer**: Joins OFS coupon codes with ERP discount amounts

*Offline/POS Discount Tracking:*
- `stg_discount_ledger_entry` - POS discount transactions from ERP
- `stg_periodic_discount` - Promotion master data (offer names, types)
- `int_discount_ledger_entry` - Aggregated discount by item ledger entry

*Order Line Integration:*
- `int_order_lines` - Discount status, amounts, and gross sales calculation
- `fact_commercial` - Final BI layer with all discount metrics

**Upstream Sources**:

*Online Flow (OFS System):*
- `mysql_ofs.inboundpaymentline` - Contains:
  - `couponcode` - Promotional code entered by customer
  - `discount` - Discount amount
  - `discounttype` - Type of discount
  - `couponamount` - Coupon value
  - `invoicediscountamount` - Invoice-level discount

*Online Flow (ERP System):*
- `sql_erp_prod_dbo.petshop_inbound_sales_line` - Contains:
  - `discount_amount` - Line item discount
  - `coupon_discount` - Coupon-specific discount
  - `invoice_discount` - Invoice-level discount

*Offline Flow (ERP System):*
- `petshop_value_entry` - Contains `discount_amount` for POS transactions
- `petshop_discount_ledger_entry` - Detailed discount transactions with offer codes
- `petshop_periodic_discount` - Promotion master (offer names, descriptions)
- `petshop_trans__sales_entry` - POS transactions with `promotion_no_`, `periodic_discount`

**Downstream Consumers**:
- Commercial performance dashboards
- Promotional effectiveness reports
- Margin analysis and pricing strategy
- Finance reconciliation and revenue reporting

**Key SQL Logic**:

### 1. Online Discount Data Stitching (int_value_entry)

**Deduplication CTE:**
```sql
WITH inbound_sales_line_dedup AS (
    SELECT
        a.documentno,
        a.item_no_,
        MAX(a.discount_amount) AS discount_amount,
        MAX(b.couponcode) AS couponcode,
        MAX(inserted_on) AS inserted_on
    FROM stg_erp_inbound_sales_line as a
    LEFT JOIN stg_ofs_inboundpaymentline as b
        ON a.item_id = b.itemid AND b.isheader = 0
    GROUP BY a.documentno, a.item_no_
)
```
- **Problem**: Multiple payment lines can exist per item (split payments, wallet + card, etc.)
- **Solution**: Deduplicate by taking MAX values, grouped by documentno + item_no_
- **Join Key**: `item_id = itemid` links ERP line to OFS payment line
- **Filter**: `isheader = 0` excludes header-level records

**Online Discount Amount Calculation:**
```sql
ROUND(COALESCE(-1 * isl.discount_amount, 0) / (1 + 5 / 100), 2) as online_discount_amount
```
- **Sign Reversal**: `discount_amount` is negative in source → multiply by -1
- **VAT Removal**: Divide by 1.05 (5% VAT rate in UAE)
- **Result**: Net discount amount excluding VAT
- **Null Handling**: COALESCE to 0 if no discount

**Coupon Code Extraction:**
```sql
isl.couponcode as online_offer_no_
```
- Sourced from OFS payment line
- Linked via deduplication CTE

### 2. Offline Discount Tracking (int_value_entry)

**Direct from Value Entry:**
```sql
ve.discount_amount as offline_discount_amount
```
- Sourced directly from value entry table
- Already VAT-exclusive in ERP

**Offer Code & Name Enrichment:**
```sql
LEFT JOIN int_discount_ledger_entry AS dic
    ON ve.item_ledger_entry_no_ = dic.item_ledger_entry_no_

-- Fields:
dic.offer_no_ as offline_offer_no_
dic.offline_offer_name
```
- Links via `item_ledger_entry_no_` (unique identifier)
- Brings in promotion code and human-readable name

### 3. Discount Classification (int_order_lines)

**Discount Status:**
```sql
CASE
    WHEN ve.sales_channel = 'Online'
         AND ve.online_discount_amount IS NOT NULL
         AND ve.online_discount_amount != 0
    THEN 'Discounted'
    WHEN ve.sales_channel != 'Online'
         AND ve.offline_discount_amount IS NOT NULL
         AND ve.offline_discount_amount != 0
    THEN 'Discounted'
    ELSE 'No Discount'
END AS discount_status
```

**Binary Flag:**
```sql
CASE
    WHEN ve.sales_channel = 'Online'
         AND ve.online_discount_amount IS NOT NULL
         AND ve.online_discount_amount != 0
    THEN 1
    WHEN ve.sales_channel != 'Online'
         AND ve.offline_discount_amount IS NOT NULL
         AND ve.offline_discount_amount != 0
    THEN 1
    ELSE 0
END AS has_discount
```

**Consolidated Amount:**
```sql
CASE
    WHEN ve.sales_channel = 'Online'
        THEN COALESCE(ve.online_discount_amount, 0)
    ELSE COALESCE(ve.offline_discount_amount, 0)
END AS discount_amount
```

### 4. Gross Sales Calculation (int_order_lines)

**For Sales Transactions:**
```sql
sales_amount__actual_ + ABS(
    CASE
        WHEN ve.sales_channel = 'Online'
            THEN COALESCE(ve.online_discount_amount, 0)
        ELSE COALESCE(ve.offline_discount_amount, 0)
    END
) AS sales_amount_gross
```
- Add discount back to get original price before discount

**For Refund Transactions:**
```sql
sales_amount__actual_ - ABS(
    CASE
        WHEN ve.sales_channel = 'Online'
            THEN COALESCE(ve.online_discount_amount, 0)
        ELSE COALESCE(ve.offline_discount_amount, 0)
    END
) AS sales_amount_gross
```
- Subtract discount to get original gross (more negative)

**Final Output:**
```sql
ROUND(
    CASE
        WHEN ve.transaction_type = 'Refund' THEN
            ve.sales_amount__actual_ - ABS(discount_amount)
        ELSE
            ve.sales_amount__actual_ + ABS(discount_amount)
    END, 2
) AS sales_amount_gross
```

### 5. Multiple Discount Aggregation (int_discount_ledger_entry)

```sql
SELECT
    item_ledger_entry_no_,

    -- Aggregate amounts
    SUM(discount_amount) as total_discount_amount,
    SUM(sales_amount) as total_sales_amount,
    SUM(quantity) as total_quantity,

    -- Combine offer names
    STRING_AGG(offline_offer_name, ', ' ORDER BY entry_no_) as combined_offer_names,

    -- Track multiple offers
    COUNT(*) as discount_count,
    CASE WHEN COUNT(*) > 1 THEN true ELSE false END as has_multiple_offers

FROM stg_discount_ledger_entry
WHERE _fivetran_deleted = false
GROUP BY item_ledger_entry_no_
```

**KPI Definitions Created**:

*Classification Metrics:*
- `discount_status` - "Discounted" vs "No Discount"
- `has_discount` - Binary flag (1 = discounted, 0 = full price)
- `transaction_type` - "Sale" vs "Refund"

*Amount Metrics:*
- `sales_amount_gross` - Pre-discount price (original price)
- `sales_amount__actual_` - Post-discount price (net revenue)
- `discount_amount` - Consolidated discount amount (online or offline)
- `online_discount_amount` - Online-specific discount (VAT-removed)
- `offline_discount_amount` - Offline-specific discount

*Offer Tracking:*
- `online_offer_no_` - Coupon/promo code from OFS
- `offline_offer_no_` - Promotion code from ERP
- `offline_offer_name` - Human-readable promotion name
- `combined_offer_names` - All offers if multiple applied

*Aggregation Metrics:*
- `discount_count` - Number of discounts on one item
- `has_multiple_offers` - Flag for offer stacking

## 3. Model Lineage (high-level)

```
ONLINE FLOW - OFS System Data Stitching:
┌────────────────────────────────────────────────────────┐
│  Source: mysql_ofs.inboundpaymentline                  │
│    ├─ couponcode (promo code)                          │
│    ├─ discount (amount)                                │
│    ├─ discounttype                                     │
│    └─ itemid (join key)                                │
└────────────────────────────────────────────────────────┘
                │
                ▼
       stg_ofs_inboundpaymentline
                │
                ├──────────────┐
                │              │
┌───────────────▼──────────────▼─────────────────────────┐
│  Source: sql_erp.petshop_inbound_sales_line            │
│    ├─ discount_amount                                  │
│    ├─ coupon_discount                                  │
│    ├─ invoice_discount                                 │
│    ├─ item_id (join key)                               │
│    └─ documentno, item_no_ (dedup keys)                │
└────────────────────────────────────────────────────────┘
                │
                ▼
       stg_erp_inbound_sales_line
                │
                ▼
    ┌──────────────────────────────┐
    │ inbound_sales_line_dedup CTE │
    │  JOIN on item_id = itemid    │
    │  GROUP BY doc + item         │
    │  MAX(discount_amount)        │
    │  MAX(couponcode)             │
    └──────────────────────────────┘
                │
                ▼
       ┌────────────────────────────────────────┐
       │     int_value_entry                    │
       │  (Stitching Layer)                     │
       │                                        │
       │  online_discount_amount =              │
       │    -1 * discount / 1.05                │
       │                                        │
       │  online_offer_no_ =                    │
       │    couponcode                          │
       └────────────────────────────────────────┘


OFFLINE FLOW - ERP Discount Ledger:
┌────────────────────────────────────────────────────────┐
│  Source: petshop_value_entry                           │
│    ├─ discount_amount                                  │
│    ├─ item_ledger_entry_no_ (join key)                 │
│    └─ sales_amount__actual_                            │
└────────────────────────────────────────────────────────┘
                │
                ▼
         stg_value_entry
                │
                │
┌───────────────▼────────────────────────────────────────┐
│  Source: petshop_discount_ledger_entry                 │
│    ├─ item_ledger_entry_no_ (join key)                 │
│    ├─ offer_no_                                        │
│    ├─ discount_amount                                  │
│    └─ entry_no_                                        │
└────────────────────────────────────────────────────────┘
                │
                ├──────────────┐
                │              │
┌───────────────▼──────────────▼─────────────────────────┐
│  Source: petshop_periodic_discount                     │
│    ├─ no_ (offer_no_)                                  │
│    ├─ description (offer_name)                         │
│    └─ discount_type                                    │
└────────────────────────────────────────────────────────┘
                │
                ▼
       stg_discount_ledger_entry
         (+ JOIN periodic_discount)
                │
                ▼
    ┌──────────────────────────────┐
    │ int_discount_ledger_entry    │
    │  GROUP BY item_ledger_entry  │
    │  SUM(discount_amount)        │
    │  STRING_AGG(offer_names)     │
    │  COUNT(*) as discount_count  │
    └──────────────────────────────┘
                │
                ▼
       ┌────────────────────────────────────────┐
       │     int_value_entry                    │
       │  (Enrichment Layer)                    │
       │                                        │
       │  offline_discount_amount =             │
       │    ve.discount_amount                  │
       │                                        │
       │  offline_offer_no_ =                   │
       │    dic.offer_no_                       │
       │                                        │
       │  offline_offer_name =                  │
       │    dic.offline_offer_name              │
       └────────────────────────────────────────┘


INTEGRATION LAYER:
       ┌────────────────────────────────────────┐
       │     int_order_lines                    │
       │  (Discount Classification)             │
       │                                        │
       │  discount_status =                     │
       │    Discounted / No Discount            │
       │                                        │
       │  has_discount =                        │
       │    1 / 0                               │
       │                                        │
       │  discount_amount =                     │
       │    online OR offline (channel-based)   │
       │                                        │
       │  sales_amount_gross =                  │
       │    actual + discount                   │
       └────────────────────────────────────────┘
                │
                ▼
         int_commercial
                │
                ▼
       fact_commercial (BI Layer)
```

## 4. Important Fields Added

### Order Line Discount Fields (int_order_lines, fact_commercial)

| Field | Description |
|-------|-------------|
| `discount_status` | "Discounted" if discount applied, "No Discount" otherwise |
| `has_discount` | Binary flag: 1 if discounted, 0 if full price |
| `discount_amount` | Consolidated discount amount (online OR offline based on channel) |
| `online_discount_amount` | Online discount (from OFS, VAT-removed) |
| `offline_discount_amount` | Offline/POS discount (from value entry) |
| `sales_amount_gross` | Pre-discount price (original price before any discounts) |
| `sales_amount__actual_` | Post-discount net revenue (actual amount received) |

### Online Offer Fields (OFS Stitching)

| Field | Description | Source |
|-------|-------------|--------|
| `online_offer_no_` | Coupon/promo code entered by customer | OFS inboundpaymentline.couponcode |
| `inserted_on` | Timestamp when order was inserted | OFS inboundpaymentline.insertedon |
| `discount` | Raw discount amount (before VAT removal) | OFS inboundpaymentline.discount |
| `discounttype` | Type of discount applied | OFS inboundpaymentline.discounttype |

### Offline Offer Fields (ERP Discount Ledger)

| Field | Description | Source |
|-------|-------------|--------|
| `offline_offer_no_` | ERP promotion code | discount_ledger_entry.offer_no_ |
| `offline_offer_name` | Human-readable promotion name | periodic_discount.description |
| `combined_offer_names` | Comma-separated list if multiple offers | STRING_AGG from discount_ledger |
| `discount_count` | Number of different discounts on item | COUNT(*) from discount_ledger |
| `has_multiple_offers` | TRUE if 2+ discounts applied simultaneously | COUNT(*) > 1 |

### POS Transaction Fields (Available in stg_erp_trans__sales_entry)

| Field | Description |
|-------|-------------|
| `promotion_no_` | POS promotion number from transaction |
| `periodic_discount` | Periodic discount amount |
| `periodic_disc__type` | Type of periodic discount |
| `periodic_disc__group` | Discount group classification |

## 5. Git Commit History Summary

| Commit ID | Author | Date | Summary |
|-----------|--------|------|---------|
| `3872656` | Anmar Abbas DataGo | 2025-06-03 | **Initial discount infrastructure**: Created discount ledger, periodic discount, online discount stitching logic |
| `3b3d38b` | Anmar Abbas DataGo | 2025-08-08 | Enhanced value entry discount enrichment |
| `6f818cd` | Anmar Abbas DataGo | 2025-10-06 | **Integrated discount into commercial models**: Added to int_order_lines, discount_status, gross sales calc |
| `6cd3a1f` | Anmar Abbas DataGo | 2025-10-15 | Refinements to order line discount logic and OFS stitching |
| `7b86fa3` | Anmar Abbas DataGo | 2025-10-25 | Latest discount tracking enhancements |

**Key Changes**:
- **3872656**: Foundation - OFS data stitching, discount ledger models, VAT removal logic
- **6f818cd**: Integration - Discount classification in order lines, gross-to-net calculation
- **6cd3a1f**: Enhancement - Deduplication improvements, multi-offer aggregation

## 6. Limitations / Assumptions

**Assumptions**:
- **VAT Rate**: 5% UAE VAT is hardcoded in online discount calculation (`/ 1.05`)
- **Sign Convention**: Online discount_amount in source is negative (logic reverses with `-1 *`)
- **Deduplication Logic**: MAX() values are correct when multiple payment lines exist
- **Join Key Stability**: `item_id = itemid` reliably links ERP to OFS records
- **Offline VAT**: Offline discount_amount is already VAT-exclusive in ERP
- **Active Discounts Only**: `_fivetran_deleted = false` filters out deleted discount records

**Limitations**:

*Data Stitching Complexity:*
- **Split Payment Handling**: When customers pay with multiple methods (card + wallet + loyalty), deduplication uses MAX() which may not always represent the correct allocation
- **Timing Issues**: OFS and ERP may have different insert timestamps for same order
- **Missing OFS Data**: If OFS payment line doesn't exist, coupon code will be NULL even if discount was applied
- **Item ID Mismatch**: Occasionally `item_id ≠ itemid` due to data quality issues

*Discount Calculation:*
- **No Margin Impact**: Discount amount tracked but not linked to cost/margin analysis
- **No Discount Percentage**: Stores absolute amounts only, not % off
- **Invoice-Level Discounts**: Not properly allocated to individual line items
- **Shipping Discounts**: Free shipping not tracked as promotional discount

*Offer Code Tracking:*
- **Manual Entry Errors**: Coupon codes depend on correct POS/online entry
- **Case Sensitivity**: Coupon codes may have inconsistent capitalization
- **Expired Offers**: No validation that offer was active on transaction date
- **Offer Stacking Rules**: No enforcement of which offers can combine

*Performance:*
- **No Incremental Logic**: Full refresh required for discount enrichment
- **Large Joins**: OFS stitching creates large intermediate tables

**Current Capabilities** (What IS Working):
✅ Stitch online discount data from OFS system to ERP
✅ Track offline discounts via discount ledger
✅ Separate online vs offline discount amounts
✅ Calculate gross (pre-discount) vs net (post-discount) revenue
✅ Capture coupon codes and promotion names
✅ Handle multiple discounts on same item
✅ VAT-exclusive discount amounts for online orders
✅ Link discounts to promotional offer master data
✅ Flag discounted vs non-discounted order lines

**Future Improvements**:

*Enhanced Data Quality:*
- Add dbt tests for discount logic:
  - `discount_amount <= sales_amount_gross`
  - `has_discount = 1` when `discount_amount > 0`
  - Online orders should have `online_discount_amount` OR NULL (not `offline_discount_amount`)
- Implement data quality checks for OFS-ERP join success rate

*Better Stitching Logic:*
- Proportional allocation for invoice-level discounts
- Handle split payment scenarios more accurately
- Add fuzzy matching for item_id when exact match fails
- Track stitching success metrics

*Margin Analysis:*
- Add `discount_percentage` = discount / gross_sales
- Calculate `gross_margin_impact` = discount amount as % of margin
- Track `discount_efficiency` = incremental revenue / discount cost

*Promotional Effectiveness:*
- Redemption rate by offer code
- Average basket size with/without discounts
- Customer acquisition cost per promotion
- Repeat purchase rate by discount type

*Incremental Processing:*
- Implement incremental models based on order date
- Optimize deduplication CTEs for performance
