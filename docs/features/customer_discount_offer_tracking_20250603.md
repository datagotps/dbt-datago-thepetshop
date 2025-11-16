# Feature Name: Discount and Offer Tracking (Offer Seeker Segmentation Foundation)

## 1. Business Purpose
Provides **comprehensive discount and promotional offer tracking** across all sales channels (Online, Shop, Affiliate, B2B, Service), enabling identification and segmentation of price-sensitive "offer seeker" customers.

**Why it was built**: To understand promotional effectiveness and customer discount behavior:
- Which customers primarily purchase with discounts vs. full price?
- What's the revenue impact of promotional offers?
- How effective are different coupon codes and promotions?
- Which channels drive the most discount-driven sales?
- What percentage of revenue comes from discounted transactions?

**Business Value**:
- **Offer Effectiveness Analysis**: Track which promotions drive sales and revenue
- **Customer Segmentation Enablement**: Identify discount-dependent vs. loyal full-price customers
- **Promotional ROI**: Measure margin impact of discount campaigns
- **Channel Performance**: Compare discount usage across online, offline, and affiliate channels
- **Pricing Strategy**: Understand price sensitivity by customer segment and product category

**Dashboard Usage**: Powers promotional performance dashboards, discount analysis reports, offer redemption tracking, and customer price sensitivity segmentation.

## 2. Technical Overview (dbt)

**Models Created/Modified**:

*Staging Layer - Discount Data:*
- `stg_discount_ledger_entry` - Offline POS discount transactions with offer details
- `stg_periodic_discount` - Promotion/offer master data (offer names, descriptions)
- `int_discount_ledger_entry` - Aggregated discount data by item ledger entry (handles multiple discounts)

*Integration Layer - Discount Enrichment:*
- `int_value_entry` - Adds online and offline discount amounts + offer codes to value entries
- `int_order_lines` - Discount status, discount amounts, and gross sales calculation

*Fact Layer:*
- `fact_commercial` - Includes all discount fields for BI reporting

**Upstream Sources**:
- `petshop_discount_ledger_entry` (ERP) - Offline/POS discount transactions
- `petshop_periodic_discount` (ERP) - Promotion master data
- `inboundpaymentline` (OFS) - Online coupon codes
- `petshop_value_entry` (ERP) - Discount amounts from value entry

**Downstream Consumers**:
- Promotional performance dashboards
- Discount effectiveness analysis
- Customer price sensitivity segmentation
- Marketing campaign ROI reporting

**Key SQL Logic**:

*Online Discount Calculation (int_value_entry):*
```sql
ROUND(COALESCE(-1 * isl.discount_amount, 0) / (1 + 5 / 100), 2) as online_discount_amount
```
- Reverses sign (discount_amount is negative in source)
- Removes 5% VAT from discount amount
- Sources from deduplicated inbound sales lines

*Offline Discount Tracking (int_value_entry):*
```sql
ve.discount_amount as offline_discount_amount
dic.offer_no_ as offline_offer_no_
dic.offline_offer_name
```
- Comes from value entry discount_amount
- Enriched with offer codes from discount ledger entry
- Includes promotional offer names from periodic discount master

*Discount Status Classification (int_order_lines):*
```sql
CASE
    WHEN ve.sales_channel = 'Online' AND ve.online_discount_amount != 0 THEN 'Discounted'
    WHEN ve.sales_channel != 'Online' AND ve.offline_discount_amount != 0 THEN 'Discounted'
    ELSE 'No Discount'
END AS discount_status
```

*Gross Sales Calculation (int_order_lines):*
```sql
-- For sales: add discount back to get original gross amount
sales_amount__actual_ + ABS(discount_amount)

-- For refunds: subtract discount to get original gross (more negative)
sales_amount__actual_ - ABS(discount_amount)
```
- Enables gross-to-net revenue analysis
- Shows impact of discounts on margin

*Multiple Discount Aggregation (int_discount_ledger_entry):*
```sql
SUM(discount_amount) as total_discount_amount
STRING_AGG(offline_offer_name, ', ') as combined_offer_names
COUNT(*) as discount_count
CASE WHEN COUNT(*) > 1 THEN true ELSE false END as has_multiple_offers
```
- Handles items with multiple simultaneous discounts
- Tracks offer stacking behavior

**KPI Definitions Created**:
- `discount_status` - Discounted vs No Discount classification
- `has_discount` - Binary flag (1/0) for discount presence
- `discount_amount` - Unified discount amount (online or offline)
- `online_discount_amount` - Online channel discount (VAT-adjusted)
- `offline_discount_amount` - Offline channel discount
- `sales_amount_gross` - Pre-discount revenue amount
- `online_offer_no_` - Online coupon/promo code
- `offline_offer_no_` - Offline promotion code
- `offline_offer_name` - Human-readable promotion name
- `has_multiple_offers` - Flag for offer stacking

## 3. Model Lineage (high-level)

```
┌─ Source: petshop_discount_ledger_entry (ERP POS discounts)
│                  │
│                  ├─► stg_discount_ledger_entry
│                  │            │
│  Source: petshop_periodic_discount (Offer master) ──┘
│                  │
│                  ▼
│          int_discount_ledger_entry (aggregated)
│                  │
│                  │
├─ Source: inboundpaymentline (OFS) ──► online coupon codes
│                  │
├─ Source: petshop_value_entry ───────► discount amounts
│                  │
│                  ▼
│          int_value_entry (discount enrichment)
│                  │
│                  ├─ online_discount_amount (VAT-adjusted)
│                  ├─ offline_discount_amount
│                  ├─ online_offer_no_ (coupon code)
│                  ├─ offline_offer_no_
│                  └─ offline_offer_name
│                  │
│                  ▼
│          int_order_lines (discount classification)
│                  │
│                  ├─ discount_status (Discounted / No Discount)
│                  ├─ has_discount (1/0 flag)
│                  ├─ discount_amount (consolidated)
│                  └─ sales_amount_gross (pre-discount)
│                  │
│                  ▼
│          int_commercial
│                  │
│                  ▼
│          fact_commercial (BI layer)
```

## 4. Important Fields Added

### Discount Classification Fields (int_order_lines, fact_commercial)

| Field | Description |
|-------|-------------|
| `discount_status` | "Discounted" if any discount applied, "No Discount" otherwise |
| `has_discount` | Binary flag: 1 if discounted, 0 if not (useful for aggregations) |
| `discount_amount` | Consolidated discount amount (online or offline based on channel) |
| `sales_amount_gross` | Original price before discount (sales_amount__actual_ + discount) |
| `sales_amount__actual_` | Final net price after discount (actual revenue) |

### Online Discount Fields (int_value_entry → fact_commercial)

| Field | Description |
|-------|-------------|
| `online_discount_amount` | Discount amount for online orders (VAT-removed: discount / 1.05) |
| `online_offer_no_` | Coupon/promo code used (from inboundpaymentline) |

### Offline Discount Fields (int_value_entry → fact_commercial)

| Field | Description |
|-------|-------------|
| `offline_discount_amount` | Discount amount from POS/offline orders (from value_entry) |
| `offline_offer_no_` | Promotion code from ERP discount ledger |
| `offline_offer_name` | Human-readable promotion name (e.g., "Summer Sale 20%") |

### Multi-Offer Tracking Fields (int_discount_ledger_entry)

| Field | Description |
|-------|-------------|
| `total_discount_amount` | Sum of all discounts if multiple offers applied |
| `combined_offer_names` | Comma-separated list of all offer names |
| `discount_count` | Number of different discounts applied to one item |
| `has_multiple_offers` | TRUE if item had 2+ simultaneous discounts |

## 5. Git Commit History Summary

| Commit ID | Author | Date | Summary |
|-----------|--------|------|---------|
| `3872656` | Anmar Abbas DataGo | 2025-06-03 | **Initial discount tracking**: Created discount ledger entry models, added online discount analysis |
| `3b3d38b` | Anmar Abbas DataGo | 2025-08-08 | Enhanced value entry with discount enrichment |
| `6f818cd` | Anmar Abbas DataGo | 2025-10-06 | Integrated discount tracking into commercial models |
| `6cd3a1f` | Anmar Abbas DataGo | 2025-10-15 | Refinements to order lines discount logic |
| `7b86fa3` | Anmar Abbas DataGo | 2025-10-25 | Latest discount tracking updates |

**Key Changes in Commits**:
- **3872656**: Foundation - Created discount ledger models, periodic discount master, online discount calculations
- **6f818cd**: Integration - Added discount fields to int_order_lines (discount_status, has_discount, gross sales calc)
- **6cd3a1f**: Enhancement - Improved discount aggregation and offer tracking logic

## 6. Limitations / Assumptions

**Assumptions**:
- Online discounts have 5% VAT included (removed via /1.05 calculation)
- Offline discounts from value_entry are VAT-exclusive
- Discount_amount in source is negative for online (sign reversed in logic)
- Coupon codes from inboundpaymentline are deduplicated by document + item
- Periodic discount master contains all active promotion details
- Discount ledger entries are not soft-deleted (_fivetran_deleted = false)

**Limitations**:
- **No customer-level aggregation**: Discount metrics exist at order line level only (not rolled up to customer segments yet)
- **No "Offer Seeker" segment**: Foundation exists, but explicit customer classification (High/Medium/Low discount dependency) not implemented
- **Limited discount type classification**: Doesn't distinguish between different discount types (percentage off, buy-one-get-one, volume discounts, etc.)
- **No margin impact tracking**: Discount amount tracked but not linked to cost/margin analysis
- **No time-based discount effectiveness**: Can't easily compare promotion performance period-over-period
- **Manual offer code entry**: Relies on correct offer code input at POS/online checkout
- **Multiple currency handling**: VAT calculation assumes AED (5% rate)

**Current Capabilities** (What IS working):
✅ Track which orders/line items have discounts applied
✅ Separate online vs offline discount amounts
✅ Capture coupon codes and offer names
✅ Calculate gross (pre-discount) vs net (post-discount) revenue
✅ Handle multiple simultaneous discounts on same item
✅ Link discounts to promotional offer master data

**Not Yet Implemented** (Offer Seeker Segmentation):
❌ Customer-level discount dependency score
❌ Offer seeker segments (Always Discount, Occasional, Full Price)
❌ Discount usage frequency metrics per customer
❌ Average discount percentage per customer
❌ Full-price vs discounted order ratio
❌ Promotional responsiveness scoring

**Future Improvements**:
- **Customer Segmentation**: Add to int_customers/dim_customers:
  - `discount_usage_rate` - % of orders with discounts
  - `avg_discount_percentage` - Average discount % when used
  - `full_price_order_ratio` - % of orders at full price
  - `offer_seeker_segment` - "High Dependency" / "Occasional" / "Full Price Buyer"
  - `promotional_responsiveness_score` - Likelihood to purchase during promotions

- **Discount Type Classification**: Category, Amount-off, Percentage-off, BOGO, Loyalty, Volume

- **Margin Impact Analysis**: Calculate gross margin with and without discounts

- **Promotion Performance Metrics**:
  - Redemption rate by offer code
  - Revenue lift during promotion periods
  - Customer acquisition cost per promotion

- **Incremental Models**: Optimize performance for large-scale discount analysis

- **dbt Tests**:
  - Generic test: discount_amount should not exceed sales_amount_gross
  - Test: online orders should have online_discount_amount or null (not offline)
  - Test: has_discount = 1 when discount_amount != 0

## 7. Enabling Offer Seeker Segmentation

### Recommended Next Steps:

To create explicit "Offer Seeker" customer segments, add to `int_customers.sql`:

```sql
-- Discount behavior metrics
SUM(CASE WHEN has_discount = 1 THEN 1 ELSE 0 END) / NULLIF(total_order_count, 0)
    AS discount_usage_rate,

AVG(CASE WHEN discount_amount > 0 THEN discount_amount / NULLIF(sales_amount_gross, 0) END)
    AS avg_discount_percentage,

-- Offer seeker segment
CASE
    WHEN discount_usage_rate >= 0.75 THEN 'High Discount Dependency'
    WHEN discount_usage_rate >= 0.40 THEN 'Moderate Offer Seeker'
    WHEN discount_usage_rate >= 0.10 THEN 'Occasional Discount User'
    ELSE 'Full Price Buyer'
END AS offer_seeker_segment
```

This would enable reporting:
- Cohort analysis: Retention rates by offer seeker segment
- CLV comparison: Full price buyers vs discount-dependent customers
- Targeted marketing: Different campaigns for each segment
- Margin optimization: Price sensitivity by customer type
