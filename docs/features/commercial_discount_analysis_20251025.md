# Feature Name: Commercial Discount Analysis

## 1. Business Purpose

**Why Built:**  
Enable comprehensive discount and promotional offer tracking across all sales channels (Online, Shop, Affiliate, B2B, Service) to support:
- Promotional campaign effectiveness analysis
- Customer discount behavior segmentation (Offer Seekers vs. Full-Price Buyers)
- Margin impact assessment from discounts
- Revenue reconciliation (gross vs. net sales)

**Insights Enabled:**
- Which channels/products have highest discount rates
- Customer lifetime discount usage patterns
- Promotional ROI by offer code/campaign
- Discount impact on gross margin and profitability

**Dashboard Consumers:**
- Commercial Analytics Dashboard (fact_commercial)
- Customer Segmentation Dashboard (dim_customers - Offer Seeker segment)
- Item Performance Dashboard (dim_items - discount metrics)
- Daily Transaction Monitoring (fct_daily_transactions)

---

## 2. Technical Overview (dbt)

### Models Created/Modified:
**Staging Layer:**
- `stg_discount_ledger_entry` - ERP offline discount transactions
- `stg_periodic_discount` - Offline offer master data (offer names/descriptions)
- `int_discount_ledger_entry` - Aggregates multiple discounts per item ledger entry

**Intermediate Layer:**
- `int_value_entry` - Calculates online/offline discount amounts with VAT adjustment
- `int_order_lines` - Consolidates discount logic and creates analysis fields
- `int_customers` - Adds customer-level discount usage metrics (orders_with_discount_count, total_discount_amount)

**Fact/Dimension Layer:**
- `fact_commercial` - Exposes 8 discount fields for analysis
- `dim_customers` - Includes Offer Seeker segmentation
- `dim_items` - Includes lifetime_discounts and discount_rate_pct

### Upstream Sources:
- `sql_erp_prod_dbo.petshop_discount_ledger_entry_*` (offline discounts)
- `sql_erp_prod_dbo.petshop_periodic_discount_*` (offline offer master)
- `stg_erp_inbound_sales_line` (online discount amounts)
- `stg_ofs_inboundpaymentline` (online coupon codes)
- `stg_value_entry` (base transaction data)

### Downstream Consumers:
- `fact_commercial` → Power BI Commercial Dashboard
- `dim_customers` → Customer Segmentation Reports
- `dim_items` → Product Performance Analysis
- `int_customers` → Customer 360 View
- `fct_daily_transactions` → Daily KPI Monitoring

### Key SQL Logic:

**Online Discount Calculation (VAT-Exclusive):**
```sql
ROUND(COALESCE(-1 * discount_amount, 0) / (1 + 5 / 100), 2) as online_discount_amount
-- Inverts sign and removes 5% VAT
```

**Offline Discount (Direct Pass-Through):**
```sql
ve.discount_amount as offline_discount_amount
-- No transformation needed
```

**Unified Discount Amount (Channel-Aware):**
```sql
CASE 
    WHEN sales_channel = 'Online' THEN online_discount_amount
    ELSE offline_discount_amount
END AS discount_amount
```

**Gross Sales Calculation:**
```sql
-- For Sales: actual_amount + discount = gross_amount
-- For Refunds: actual_amount - discount = gross_amount
sales_amount__actual_ + ABS(discount_amount) AS sales_amount_gross
```

**Discount Flag:**
```sql
CASE 
    WHEN discount_amount != 0 THEN 1 
    ELSE 0 
END AS has_discount
```

### KPI Definitions Created:
- **discount_status** - Categorical: 'Discounted' or 'No Discount'
- **has_discount** - Binary flag: 1 (discounted) or 0 (not discounted)
- **discount_amount** - Unified discount value in AED
- **sales_amount_gross** - Original price before discount
- **discount_rate_pct** - (discounted_transactions / total_transactions) * 100
- **lifetime_discounts** - Total discount amount given per item/customer
- **orders_with_discount_count** - Number of orders using discounts per customer

---

## 3. Model Lineage (High-Level)

```
SOURCES
├── petshop_discount_ledger_entry (ERP)
├── petshop_periodic_discount (ERP)
├── petshop_inbound_sales_line (ERP)
└── ofs_inboundpaymentline (OFS)
    ↓
STAGING
├── stg_discount_ledger_entry
├── stg_periodic_discount
├── stg_erp_inbound_sales_line
├── stg_ofs_inboundpaymentline
└── stg_value_entry
    ↓
INTERMEDIATE
├── int_discount_ledger_entry (aggregates offline discounts)
├── int_value_entry (calculates online/offline discount amounts)
├── int_order_lines (consolidates & creates analysis fields)
├── int_commercial (pass-through)
└── int_customers (customer-level discount metrics)
    ↓
MARTS
├── fact_commercial (transaction-level discount analysis)
├── dim_customers (customer discount behavior)
├── dim_items (item discount performance)
└── fct_daily_transactions (daily discount monitoring)
```

### Complete Source Table Mapping

| Component | Channel | Source Table(s) | Join Key | Notes |
|-----------|---------|-----------------|----------|-------|
| **Discount Amount** | Online | `stg_erp_inbound_sales_line` | `documentno` + `item_no_` | VAT-adjusted, deduplicated |
| **Discount Identifier** | Online | `stg_ofs_inboundpaymentline` | `item_id` = `itemid` | Coupon code |
| **Discount Amount** | Offline | `stg_value_entry` | N/A (direct) | No transformation |
| **Discount Identifier** | Offline | `int_discount_ledger_entry` | `item_ledger_entry_no_` | Offer code + offer name |

---

## 4. Important Fields Added

### In `fact_commercial`:
- **discount_status** - Text label: 'Discounted' or 'No Discount' (for filtering)
- **has_discount** - Binary 0/1 flag (for counting/aggregations)
- **discount_amount** - Unified discount in AED (channel-aware)
- **online_discount_amount** - Online-specific discount (VAT-exclusive)
- **offline_discount_amount** - Offline-specific discount
- **online_offer_no_** - Online coupon code (e.g., "SAVE20")
- **offline_offer_no_** - Offline offer number (e.g., "OFF001")
- **offline_offer_name** - Offline offer description (e.g., "Black Friday Sale")
- **sales_amount_gross** - Original price before discount (enables discount % calculation)

### In `dim_customers`:
- **orders_with_discount_count** - Total orders using discounts
- **total_discount_amount** - Lifetime discount amount received
- **offer_seeker_segment** - 'Offer Seeker', 'Mixed', or 'Full-Price Buyer'

### In `dim_items`:
- **lifetime_discounts** - Total discount given for this item
- **discount_rate_pct** - % of transactions that were discounted

---

## 5. Git Commit History Summary (Major Changes Only)

| Commit ID | Date | Author | Summary |
|-----------|------|--------|---------|
| **7b86fa3** | 2025-10-25 | Anmar Abbas DataGo | **Initial discount feature implementation.** Created 3 new staging models (`stg_discount_ledger_entry`, `int_discount_ledger_entry`, `stg_periodic_discount`) to capture offline discounts from ERP. Modified `int_value_entry` to calculate online/offline discount amounts with VAT adjustment logic. Added discount fields to `int_order_lines` and exposed 8 discount dimensions/facts in `fact_commercial`. |
| **393095d** | 2025-11-09 | datagotps | **Added customer-level discount analytics.** Created new CTE `customer_offer_usage` in `int_customers` to aggregate discount metrics per customer (orders_with_discount_count, total_discount_amount, distinct_offers_used). Added `offer_seeker_segment` business logic to classify customers as 'Offer Seeker', 'Occasional Offer User', or 'Non-Offer User' based on discount usage patterns. Exposed these fields in `dim_customers`. |

---

## 6. Limitations / Assumptions

### Assumptions:
1. **VAT Rate:** Online discounts assume 5% VAT rate (hardcoded in calculation)
2. **Channel Classification:** Sales channel determined by `clc_global_dimension_2_code_name` field
3. **Deduplication:** Online discounts deduplicated by `documentno` + `item_no_` using MAX value
4. **Offer Aggregation:** Multiple offline offers per transaction are aggregated (sum of discount amounts)
5. **Legacy Logic:** Replaced legacy `ONline_discounts_master.sql` logic with new standardized approach

### Limitations:
1. **Historical Data:** Discount tracking only available from 2021-06-01 onwards (based on legacy logic)
2. **Coupon Code Granularity:** Online coupon codes may be NULL if not captured in payment line
3. **Offline Offer Names:** Dependent on `stg_periodic_discount` master data completeness
4. **Multi-Offer Transactions:** Offline transactions with multiple offers show combined discount (individual offer breakdown not preserved in fact table)
5. **Refund Handling:** Discount logic for refunds assumes original discount amount is preserved

### Future Improvements:
- Add discount type categorization (percentage vs. fixed amount)
- Track discount source (manual, automatic, loyalty program)
- Add time-based discount effectiveness metrics
- Create discount campaign performance mart
- Add discount budget tracking and forecasting
- Implement discount abuse detection logic

---

**Documentation Generated:** 2025-11-16  
**Feature Owner:** Commercial Analytics Team  
**Last Updated By:** Anmar Abbas DataGo
