# Microsoft Dynamics Business Central - Sales & Discount Data Model

**Date:** 2024-11-19  
**ERP System:** Microsoft Dynamics Business Central  
**Purpose:** Document the data flow from ERP source tables through to final customer metrics

---

## 📊 Overview: Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Microsoft Dynamics BC ERP                    │
│                         Source Tables                            │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  LAYER 1: STAGING (stg_)                                         │
│  - stg_value_entry (core sales/cost data)                       │
│  - stg_erp_inbound_sales_line (online discounts)                │
│  - stg_discount_ledger_entry (offline discounts)                │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  LAYER 2: INTERMEDIATE (int_)                                    │
│  - int_value_entry (enriched with discounts)                    │
│  - int_order_lines (line-level sales & discounts)               │
│  - int_orders (order-level aggregation)                         │
│  - int_customers (customer-level aggregation)                   │
└───────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│  LAYER 3: FACT/DIM (fct_ / dim_)                                │
│  - dim_customers (final customer dimension)                     │
│  - fct_orders (order facts)                                     │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🗄️ Source: Microsoft Dynamics Business Central Tables

### Primary Source Table: **Value Entry**

**Table Names in BigQuery:**
- `petshop_value_entry_437dbf0e_84ff_417a_965d_ed2bb9650972`
- `pethaus_domestic_grooming_value_entry_437dbf0e_84ff_417a_965d_ed2bb9650972`
- `pethaus_general_trading_value_entry_437dbf0e_84ff_417a_965d_ed2bb9650972`
- `the_petshop_cafe_value_entry_437dbf0e_84ff_417a_965d_ed2bb9650972`

**Key ERP Source Columns:**

| ERP Column | Data Type | Description |
|------------|-----------|-------------|
| `entry_no_` | INTEGER | Unique entry identifier |
| `document_no_` | STRING | Invoice/order number |
| `document_type` | INTEGER | 2=Sales Invoice, 4=Credit Memo |
| `item_ledger_entry_no_` | INTEGER | Links to item ledger |
| `item_no_` | STRING | Product SKU |
| `source_no_` | STRING | Customer ID |
| `posting_date` | DATETIME | Transaction date |
| `invoiced_quantity` | BIGNUMERIC | Quantity sold/returned |
| **`sales_amount__actual_`** | **BIGNUMERIC** | **Net sales value (after discount)** |
| `sales_amount__expected_` | BIGNUMERIC | Expected sales value |
| **`discount_amount`** | **BIGNUMERIC** | **Offline discount (negative value)** |
| `cost_amount__actual_` | BIGNUMERIC | Cost of goods sold |
| `global_dimension_1_code` | STRING | Store location |
| `global_dimension_2_code` | STRING | Profit center/channel |

---

## 🔄 LAYER 1: Staging Layer (`stg_value_entry`)

### Purpose
Raw data extraction from ERP with minimal transformation.

### Key Transformations
- **Union** multiple company sources (Pet Shop, Pet Haus, TPS Café)
- **Map** dimension codes to readable names
- **Clean** data types and formats

```sql
-- stg_value_entry.sql (lines 64-71)
-- Financial Amounts directly from ERP
a.sales_amount__actual_,      -- Net sales (post-discount)
a.sales_amount__expected_,
a.purchase_amount__actual_,
a.purchase_amount__expected_,
a.cost_amount__actual_,
a.cost_amount__expected_,
a.cost_amount__non_invtbl__,
a.discount_amount,             -- Offline discount from ERP
```

### Source Distinction
1. **Offline Discounts** → Stored in `value_entry.discount_amount`
2. **Online Discounts** → Stored separately in `inbound_sales_line` table

---

## 🔧 LAYER 2a: `int_value_entry`

### Purpose
Enrich value entry with discount information from multiple sources.

### Discount Calculation Logic

#### **ONLINE Discounts** (lines 205-206)
```sql
-- From OFS (Online Fulfillment System) via inbound_sales_line
ROUND(COALESCE(-1 * isl.discount_amount, 0) / (1 + 5 / 100), 2) as online_discount_amount
```
**Logic:**
- Source: `stg_erp_inbound_sales_line.discount_amount`
- Convert from positive to negative: `-1 *`
- Remove 5% VAT: `/ (1 + 5/100)`
- Round to 2 decimals

**Example:**
- OFS discount: 105 AED (includes VAT)
- Calculation: `-1 * 105 / 1.05 = -100 AED`

#### **OFFLINE Discounts** (line 208)
```sql
-- Directly from ERP value_entry (already net of VAT)
ve.discount_amount as offline_discount_amount
```
**Logic:**
- Source: `stg_value_entry.discount_amount`
- Already stored as negative value in ERP
- No VAT adjustment needed

#### **Discount Metadata**
```sql
-- Online offer tracking
isl.couponcode as online_offer_no_        -- From inbound_sales_line

-- Offline offer tracking
dic.offer_no_ as offline_offer_no_        -- From discount_ledger_entry
dic.offline_offer_name                     -- Offer description
```

### Data Sources Joined
1. **`stg_value_entry`** → Core sales/cost data
2. **`inbound_sales_line_dedup`** → Online discount amounts & coupon codes
3. **`int_discount_ledger_entry`** → Offline discount offers & names
4. **`int_inbound_sales_header`** → Online order metadata
5. **`int_erp_customer`** → Customer information
6. **`int_items`** → Product information

---

## 🔧 LAYER 2b: `int_order_lines`

### Purpose
Create line-item level view with consolidated discount logic.

### Consolidated Discount Amount (lines 54-59)
```sql
-- Unified discount field (chooses online OR offline)
CASE 
    WHEN ve.sales_channel = 'Online' 
        THEN COALESCE(ve.online_discount_amount, 0)
    ELSE COALESCE(ve.offline_discount_amount, 0)
END AS discount_amount
```

**Business Rule:**
- **Online orders** → Use `online_discount_amount` (from OFS)
- **Shop/Affiliate orders** → Use `offline_discount_amount` (from ERP)

### Discount Status Classification (lines 41-52)
```sql
-- Discount Status: 'Discounted' or 'No Discount'
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

-- Has Discount Flag: 1 or 0
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

### Gross Sales Calculation (lines 73-94)
```sql
-- Calculate original price before discount
ROUND(
    CASE 
        WHEN ve.transaction_type = 'Refund' THEN 
            -- For refunds: subtract discount (more negative)
            ve.sales_amount__actual_ - ABS(discount_amount)
        ELSE 
            -- For sales: add discount back (original price)
            ve.sales_amount__actual_ + ABS(discount_amount)
    END, 2
) AS sales_amount_gross
```

**Formula:**
- **Gross Sales** = Net Sales + Discount Amount
- **Net Sales** = `sales_amount__actual_` (from ERP)
- **Discount** = Absolute value of discount (already negative)

**Example:**
- Net Sales: 95 AED
- Discount: -5 AED
- Gross Sales: 95 + 5 = 100 AED

---

## 🔧 LAYER 2c: `int_orders`

### Purpose
Aggregate line items to order level.

### Order-Level Discount Metrics
```sql
-- From int_orders aggregation
SUM(sales_amount__actual_) as order_value          -- Net order value
SUM(sales_amount_gross) as order_gross_value       -- Gross before discount
SUM(ABS(discount_amount)) as order_discount_amount -- Total discount
COUNT(DISTINCT online_offer_no_) as offers_used    -- Number of offers
```

---

## 🔧 LAYER 2d: `int_customers` (WITH VOUCHER FIX)

### Purpose
Aggregate orders to customer level with corrected discount logic.

### Customer-Level Discount Calculation (lines 231-239)

#### **UPDATED LOGIC (Post-Fix):**
```sql
-- Total discount amount (cap discount at sales value to exclude voucher overflow)
ROUND(SUM(CASE 
    WHEN ol.transaction_type = 'Sale' 
        AND ol.sales_amount__actual_ > 0  
    -- Only count discount up to the sales amount (prevents voucher overflow)
    THEN LEAST(
        ABS(COALESCE(ol.discount_amount, 0)),
        ABS(COALESCE(ol.sales_amount__actual_, 0))
    )
    ELSE 0
END), 0) AS total_discount_amount
```

**Why This Fix?**
- **Problem:** Voucher/credit line items had `sales_amount = 0` but large `discount_amount`
- **Impact:** Inflated `total_discount_amount` beyond `total_sales_value`
- **Solution:** Cap discount at sales value using `LEAST()` function

**Before Fix:**
```
Item: Voucher Booklet (206216-1)
sales_amount__actual_ = 0.0
discount_amount = -2857.14
COUNTED IN TOTAL: 2857.14 ❌ (wrong!)
```

**After Fix:**
```
Item: Voucher Booklet (206216-1)
sales_amount__actual_ = 0.0
discount_amount = -2857.14
COUNTED IN TOTAL: LEAST(2857.14, 0.0) = 0.0 ✅ (correct!)
```

### Customer Discount Metrics

| Metric | Formula | Description |
|--------|---------|-------------|
| `total_sales_value` | `SUM(order_value)` | Total net sales from `int_orders` |
| `total_discount_amount` | `SUM(LEAST(discount, sales))` | Total discounts capped at sales value |
| `orders_with_discount_count` | `COUNT(DISTINCT CASE...)` | Orders with any discount |
| `distinct_offers_used` | `COUNT(DISTINCT offer_no_)` | Unique offers redeemed |
| `discount_usage_rate_pct` | `(orders_with_discount / total_orders) * 100` | % of orders with discount |
| `discount_dependency_pct` | `(total_discount / total_sales) * 100` | Discount as % of sales |
| `discount_affinity_score` | `(usage_rate * 0.5) + (dependency * 0.3) + (offers * 5)` | Composite 0-100 score |

---

## 📐 Business Rules Summary

### 1. **Online vs Offline Discount Sources**
- **Online:** From OFS system (`inbound_sales_line`)
  - Includes VAT, must be removed
  - Tracked with coupon codes
- **Offline:** From ERP (`value_entry`)
  - Net of VAT
  - Tracked with offer numbers from `discount_ledger_entry`

### 2. **Discount Sign Convention**
- **ERP stores discounts as NEGATIVE** values
- **We use ABS()** to display as positive amounts
- **Formula:** `Gross = Net + ABS(Discount)`

### 3. **Voucher/Credit Handling**
- **Vouchers** are NOT product discounts
- **Excluded** from discount calculations
- **Identified** by: `sales_amount = 0` but `discount_amount != 0`

### 4. **Transaction Types**
- **Sale** (document_type = 2): Positive sales & discounts
- **Refund** (document_type = 4): Negative sales & discounts
- **Gross calculation** adjusts for transaction type

### 5. **Data Granularity**
- **Order Lines:** Item-level discount detail
- **Orders:** Aggregated discount per order
- **Customers:** Total lifetime discount behavior

---

## 🔍 Key ERP Tables Reference

| ERP Table | Purpose | Key Columns |
|-----------|---------|-------------|
| **Value Entry** | Core transaction ledger | `sales_amount__actual_`, `discount_amount`, `cost_amount__actual_` |
| **Discount Ledger Entry** | Offline offer tracking | `offer_no_`, `offline_offer_name`, `item_ledger_entry_no_` |
| **Inbound Sales Line** | Online order details | `discount_amount`, `couponcode`, `documentno` |
| **Inbound Sales Header** | Online order metadata | `web_order_id`, `online_order_channel`, `order_type` |
| **Customer** | Customer master | `no_`, `name`, `phone_no_`, `loyality_member_id` |
| **Item** | Product master | `item_no_`, `description`, `item_category_code` |

---

## 📊 Data Quality & Validation

### Current Status (Post-Fix)
- ✅ Discount dependency capped at reasonable values (max 110% vs 16,939%)
- ✅ Voucher overflow eliminated
- ✅ Only 0.04% of customers have dependency > 100% (acceptable edge cases)
- ✅ Clean customer segmentation based on actual discount behavior

### Monitoring Recommendations
1. **Alert** if `discount_dependency_pct > 110%` for any customer
2. **Review** items with pattern: `sales_amount = 0` AND `discount_amount != 0`
3. **Track** new voucher items to ensure proper exclusion
4. **Validate** online discount VAT adjustment remains at 5%

---

## 🎯 Summary: ERP Source → Final Metrics

```
Microsoft Dynamics BC ERP
  ↓
petshop_value_entry (table)
  ├── sales_amount__actual_     → Net Sales
  ├── discount_amount            → Offline Discount
  └── cost_amount__actual_       → COGS
  ↓
stg_value_entry (staging)
  ↓
int_value_entry (enriched)
  ├── online_discount_amount    ← From inbound_sales_line
  ├── offline_discount_amount   ← From value_entry.discount_amount
  └── online_offer_no_          ← From inbound_sales_line.couponcode
  ↓
int_order_lines (line-level)
  ├── discount_amount           ← Consolidated online/offline
  ├── sales_amount_gross        ← Net + Discount
  └── has_discount              ← Boolean flag
  ↓
int_orders (order-level)
  ├── order_value               ← SUM(net sales)
  └── order_discount_amount     ← SUM(discounts)
  ↓
int_customers (customer-level)
  ├── total_sales_value         ← From int_orders
  ├── total_discount_amount     ← From int_order_lines (capped at sales)
  ├── discount_dependency_pct   ← (discount / sales) * 100
  └── discount_affinity_score   ← Composite metric
  ↓
dim_customers (final dimension)
  └── All discount metrics exposed for analysis
```

---

**Document Version:** 1.0  
**Last Updated:** 2024-11-19  
**Maintained By:** Data Engineering Team

