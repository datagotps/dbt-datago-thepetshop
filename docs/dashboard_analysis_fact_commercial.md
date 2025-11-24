# Dashboard Analysis for: `fact_commercial`

## Executive Summary

`fact_commercial` is the **gold layer** commercial transaction fact table for The Pet Shop group. It provides a comprehensive, denormalized view of all sales transactions across multiple business entities (Pet Shop, Pet Haus, TPS Cafe, Pet Shop Services). The model integrates ERP value entries, online order fulfillment data, customer information, product hierarchy, and discount tracking to enable multi-dimensional commercial analysis.

**Business Purpose**: Enable commercial performance monitoring, sales channel analysis, discount effectiveness tracking, product performance reporting, and time-comparative analytics across the entire pet retail business.

---

## 1. Data Lineage Analysis

### 1.1 Source to Target Flow

```
[Raw ERP Sources]                    [Raw OFS Sources]              [Loyalty/Discount Sources]
       |                                    |                                |
       v                                    v                                v
+------------------+              +----------------------+        +------------------------+
| petshop_value_   |              | inboundsalesheader   |        | discount_ledger_entry  |
| entry (4 sources)|              | inboundsalesline     |        | periodic_discount      |
| - Pet Shop       |              | inboundpaymentline   |        | loyalty_members        |
| - Pet Haus       |              | crmorders            |        +------------------------+
| - TPS Cafe       |              +----------------------+
| - Services       |                       |
+------------------+                       v
        |                        +----------------------+
        v                        | stg_erp_inbound_*    |
+------------------+             | stg_ofs_inbound*     |
| stg_value_entry  |             +----------------------+
| stg_dimension_*  |                       |
| stg_pethaus_*    |                       v
| stg_cafe_*       |             +----------------------+
+------------------+             | int_inbound_sales_   |
        |                        | header               |
        |                        +----------------------+
        |                                  |
        v                                  |
+------------------+    +-------------+    |    +-------------------+
| int_value_entry  |<---| int_items   |<---|<---| int_erp_customer  |
+------------------+    +-------------+    |    +-------------------+
        |                                  |
        v                                  v
+--------------------------------------------------+
|              int_order_lines                      |
| (Adds: discount calcs, time flags, unified IDs)  |
+--------------------------------------------------+
        |
        v
+--------------------------------------------------+
|              int_commercial                       |
|              (Pass-through)                       |
+--------------------------------------------------+
        |
        v
+==================================================+
|              fact_commercial                      |
|         (GOLD LAYER - Final Output)              |
+==================================================+
```

### 1.2 Source Tables Reference

| Source Layer | Source Table | Type | Description | Key Fields |
|--------------|--------------|------|-------------|------------|
| Raw ERP | `petshop_value_entry_*` | BigQuery | Core transaction ledger entries | entry_no_, document_no_, amounts |
| Raw ERP | `pethaus_*_value_entry_*` | BigQuery | Pet Haus grooming transactions | Same structure as petshop |
| Raw ERP | `the_petshop_cafe_value_entry_*` | BigQuery | Cafe transactions | Same structure as petshop |
| Raw ERP | `petshop_trans__sales_entry_*` | BigQuery | Service (grooming) transactions | transaction_no_, amounts |
| Raw OFS | `inboundsalesheader` | MySQL/OFS | Online order headers | weborderno, ordertype, payment |
| Raw OFS | `inboundsalesline` | MySQL/OFS | Online order line items | documentno, item_no_, discount |
| Raw OFS | `inboundpaymentline` | MySQL/OFS | Payment and coupon info | itemid, couponcode |
| Raw ERP | `petshop_customer_*` | BigQuery | Customer master data | no_, name, phone |
| Raw ERP | `petshop_item_*` | BigQuery | Item master data | item_no_, category, brand |
| Raw ERP | `petshop_discount_ledger_*` | BigQuery | Offline discount entries | offer_no_, offer_name |
| Raw ERP | `petshop_dimension_value_*` | BigQuery | Dimension mappings | code, name, dimension_code |

### 1.3 Key Transformation Logic

| Transformation | Location | Business Logic |
|----------------|----------|----------------|
| **Company Source** | `stg_value_entry` | Union of 4 sources: Pet Shop, Pet Haus, TPS Cafe, Services |
| **Sales Channel** | `int_value_entry` | Derived from dimension_2_code: Online, Shop, Affiliate, B2B, Service |
| **Transaction Type** | `int_value_entry` | Sale/Refund based on document_type and amount sign |
| **Discount Calculation** | `int_order_lines` | Online: from inbound_sales_line; Offline: from value_entry.discount_amount |
| **Gross Sales** | `int_order_lines` | Net sales + discount (adjusts for refunds) |
| **Location City** | `int_order_lines` | Mapped: REM/FZN = Abu Dhabi, RAK = Ras Al Khaimah, else Dubai |
| **Unified Customer ID** | `int_order_lines` | Phone number if valid, else source_no_ |
| **Time Period Flags** | `int_order_lines` | MTD, YTD, LMTD, LYTD, M_1, M_2, M_3, Y_1, Y_2 |

---

## 2. Data Dictionary

### 2.1 Core Identifiers

| Column | Data Type | Description | Example Values |
|--------|-----------|-------------|----------------|
| `source_no_` | STRING | Customer ID from ERP | 'CUST-0001' |
| `document_no_` | STRING | ERP document number | 'PSI/2024/01234' |
| `posting_date` | DATE | Transaction posting date | 2024-01-15 |
| `item_ledger_entry_no_` | INTEGER | Unique ledger entry ID | 12345678 |
| `unified_order_id` | STRING | Standardized order identifier | 'O1234567S' or 'DIP-DT08-48383' |
| `unified_refund_id` | STRING | Refund document number | 'PSC/2024/00567' |
| `unified_customer_id` | STRING | Unified customer key (phone or source_no_) | '971501234567' |

### 2.2 Sales Channel Dimensions

| Column | Data Type | Description | Possible Values |
|--------|-----------|-------------|-----------------|
| `company_source` | STRING | Business entity | Pet Shop, Pet Haus, TPS Cafe, Pet Shop Services |
| `sales_channel` | STRING | Primary sales channel | Online, Shop, Affiliate, B2B, Service |
| `sales_channel_sort` | INTEGER | Sort order for channels | 1-6 |
| `transaction_type` | STRING | Transaction classification | Sale, Refund, Other |
| `offline_order_channel` | STRING | Store code for POS sales | DIP, FZN, REM, UMSQ, WSL, CREEK, DSO, MRI, RAK |
| `online_order_channel` | STRING | Online platform | website, Android, iOS, CRM, Unmapped |
| `sales_channel_detail` | STRING | Detailed channel breakdown | Store code, platform name, or profit center |
| `affiliate_order_channel` | STRING | Affiliate partner name | Amazon, Noon, Talabat, Deliveroo, etc. |

### 2.3 Financial Metrics (Facts)

| Column | Data Type | Description | Business Rule |
|--------|-----------|-------------|---------------|
| `invoiced_quantity` | DECIMAL | Quantity sold/returned | Negative for refunds |
| `sales_amount_gross` | DECIMAL | Revenue before discounts (AED) | Net + Discount |
| `sales_amount__actual_` | DECIMAL | Net revenue after discounts (AED) | From ERP value entry |
| `cost_amount__actual_` | DECIMAL | Cost of goods sold (AED) | From ERP value entry |
| `discount_amount` | DECIMAL | Total discount applied (AED) | Consolidated online/offline |
| `has_discount` | INTEGER | Discount indicator flag | 0 = No, 1 = Yes |
| `online_discount_amount` | DECIMAL | Discount for online orders (AED) | VAT-adjusted from OFS |
| `offline_discount_amount` | DECIMAL | Discount for offline orders (AED) | From ERP discount field |

### 2.4 Discount Dimensions

| Column | Data Type | Description | Example Values |
|--------|-----------|-------------|----------------|
| `discount_status` | STRING | Whether discounted | Discounted, No Discount |
| `online_offer_no_` | STRING | Online promo/coupon code | 'SAVE20' |
| `offline_offer_no_` | STRING | Offline promotion ID | 'PROMO-001' |
| `offline_offer_name` | STRING | Offline promotion name | 'Weekend Special' |

### 2.5 Product Dimensions

| Column | Data Type | Description | Example Values |
|--------|-----------|-------------|----------------|
| `item_no_` | STRING | Product SKU | '100001-1' |
| `item_name` | STRING | Product description | 'Royal Canin Adult Dog Food 15kg' |
| `division` | STRING | Top product hierarchy | DOG, CAT, FISH, BIRD, SMALL PET, REPTILE, SERVICE |
| `item_category` | STRING | Product category | FOOD, ACCESSORIES, HEALTH & HYGIENE |
| `item_subcategory` | STRING | Product subcategory | Dry Food, Wet Food, Treats, Toys |
| `item_type` | STRING | Detailed item type | Specific product classification |
| `item_brand` | STRING | Brand name | Royal Canin, Hill's, Pedigree |
| `division_sort_order` | INTEGER | Division display order | 1-999 |
| `item_category_sort_order` | INTEGER | Category display order | 1-999 |

### 2.6 Location Dimensions

| Column | Data Type | Description | Possible Values |
|--------|-----------|-------------|-----------------|
| `location_code` | STRING | Store/warehouse code | DIP, FZN, REM, UMSQ, WSL, CREEK, DSO, MRI, RAK |
| `clc_location_code` | STRING | Calculated location (adjusted) | Same as above, with special mappings |
| `location_city` | STRING | City classification | Dubai, Abu Dhabi, Ras Al Khaimah |

### 2.7 Customer Dimensions

| Column | Data Type | Description | Example Values |
|--------|-----------|-------------|----------------|
| `customer_name` | STRING | Customer full name | 'John Doe' |
| `std_phone_no_` | STRING | Standardized phone (E.164) | '971501234567' |
| `raw_phone_no_` | STRING | Original phone number | '+971 50 123 4567' |
| `duplicate_flag` | STRING | Duplicate customer indicator | Yes, No |
| `customer_identity_status` | STRING | Verification status | Verified, Unverified |
| `loyality_member_id` | STRING | Loyalty program ID | 'LM-00001' |

### 2.8 Online Order Dimensions

| Column | Data Type | Description | Possible Values |
|--------|-----------|-------------|-----------------|
| `web_order_id` | STRING | Online order number | 'O1234567S' |
| `order_type` | STRING | Delivery type | EXPRESS, NORMAL, EXCHANGE |
| `paymentgateway` | STRING | Payment method used | creditCard, cash, COD, Tabby, Loyalty |
| `paymentmethodcode` | STRING | Payment method category | PREPAID, COD |

### 2.9 Posting Group Dimensions

| Column | Data Type | Description | Purpose |
|--------|-----------|-------------|---------|
| `gen__prod__posting_group` | STRING | General product posting group | GL mapping |
| `gen__bus__posting_group` | STRING | General business posting group | GL mapping |
| `source_posting_group` | STRING | Source posting group | Customer/vendor classification |
| `inventory_posting_group` | STRING | Inventory posting group | Stock valuation |
| `global_dimension_1_code` | STRING | Store dimension | Store analysis |
| `global_dimension_2_code` | STRING | Profit center dimension | Profitability analysis |
| `dimension_code` | STRING | Primary dimension type | PROFITCENTER (filtered) |
| `global_dimension_2_code_name` | STRING | Dimension 2 readable name | Profit center name |
| `clc_global_dimension_2_code_name` | STRING | Calculated dimension 2 name | Normalized profit center |

### 2.10 Time Period Flags (Facts)

| Column | Data Type | Description | Values |
|--------|-----------|-------------|--------|
| `is_mtd` | INTEGER | Month-to-Date flag | 0 or 1 |
| `is_ytd` | INTEGER | Year-to-Date flag | 0 or 1 |
| `is_lmtd` | INTEGER | Last Month-to-Date flag | 0 or 1 |
| `is_lymtd` | INTEGER | Last Year Month-to-Date flag | 0 or 1 |
| `is_lytd` | INTEGER | Last Year-to-Date flag | 0 or 1 |
| `is_m_1` | INTEGER | Full Last Month flag | 0 or 1 |
| `is_m_2` | INTEGER | Full Month-2 flag | 0 or 1 |
| `is_m_3` | INTEGER | Full Month-3 flag | 0 or 1 |
| `is_y_1` | INTEGER | Full Last Year flag | 0 or 1 |
| `is_y_2` | INTEGER | Full Year-2 flag | 0 or 1 |

### 2.11 System Fields

| Column | Data Type | Description |
|--------|-----------|-------------|
| `report_last_updated_at` | DATETIME | Report refresh timestamp (UTC+4) |
| `user_id` | STRING | ERP user who created transaction |
| `entry_type` | STRING | Entry classification (Direct Cost, Revaluation, Rounding) |
| `document_type` | STRING | Document type (Sales Invoice, Sales Credit Memo) |
| `source_code` | STRING | Transaction source (BACKOFFICE, SALES) |

---

## 3. Dashboard Strategy

### 3.1 Target Audience

| Audience | Role | Key Interests |
|----------|------|---------------|
| **Executives (C-Suite)** | CEO, CFO, COO | Total revenue, margin trends, YoY growth, channel mix |
| **Commercial Directors** | Head of Sales, Retail Director | Channel performance, store rankings, customer acquisition |
| **Category Managers** | Product/Division Managers | Category sales, brand performance, inventory turnover |
| **Marketing Team** | Marketing Manager, CRM Lead | Discount effectiveness, customer behavior, campaign ROI |
| **Operations** | Store Managers, Ops Team | Store performance, transaction volumes, product mix |
| **Finance** | Finance Controller | Revenue recognition, cost analysis, discount impact |

### 3.2 Key Business Questions

1. **Revenue Performance**: What is our total revenue MTD/YTD vs. same period last year?
2. **Channel Analysis**: How do Online vs. Shop vs. Affiliate channels compare?
3. **Discount Impact**: What percentage of revenue is discounted? What's the discount-to-sales ratio?
4. **Product Performance**: Which divisions/categories/brands are driving growth?
5. **Store Performance**: Which stores are over/underperforming?
6. **Customer Trends**: Are we acquiring new customers? What's the repeat rate?
7. **Margin Analysis**: What's our gross margin trend by channel and category?
8. **Geographic Analysis**: How do Dubai, Abu Dhabi, and RAK compare?

### 3.3 Primary KPIs

| KPI | Formula | Target Use |
|-----|---------|------------|
| **Net Revenue** | SUM(sales_amount__actual_) WHERE transaction_type = 'Sale' | Primary performance metric |
| **Gross Revenue** | SUM(sales_amount_gross) WHERE transaction_type = 'Sale' | Full value before discounts |
| **Gross Margin %** | (Net Revenue - COGS) / Net Revenue * 100 | Profitability indicator |
| **Discount Rate %** | SUM(discount_amount) / SUM(sales_amount_gross) * 100 | Promotional efficiency |
| **Average Transaction Value (ATV)** | Net Revenue / COUNT(DISTINCT document_no_) | Basket size metric |
| **Units Sold** | SUM(invoiced_quantity) WHERE transaction_type = 'Sale' | Volume indicator |

---

## 4. Dashboard Design Specification

### Tab 1: Executive Summary

**Purpose**: High-level business health overview for C-suite and senior leadership.

**Layout**:
- Row 1: KPI Cards (4 across)
- Row 2: Revenue Trend (60%) + Channel Mix Donut (40%)
- Row 3: Division Performance Bar + Geographic Heatmap

**Visualizations**:

| Visual | Type | Metrics | Dimensions | Description |
|--------|------|---------|------------|-------------|
| **Net Revenue MTD** | KPI Card | SUM(sales_amount__actual_) WHERE is_mtd=1 | - | Month-to-date revenue with vs. LY |
| **YoY Growth %** | KPI Card | (MTD - LYMTD) / LYMTD * 100 | - | Year-over-year growth percentage |
| **Gross Margin %** | KPI Card | (Revenue - COGS) / Revenue | - | Overall gross margin |
| **Discount Rate %** | KPI Card | discount_amount / sales_amount_gross | - | Promotional intensity |
| **Revenue Trend** | Area Chart | Net Revenue, COGS | posting_date (monthly) | 12-month rolling revenue with cost overlay |
| **Channel Mix** | Donut Chart | Net Revenue | sales_channel | Share of revenue by channel |
| **Division Performance** | Horizontal Bar | Net Revenue, Margin % | division | Sorted by revenue, color by margin |
| **Geographic Heatmap** | Map/Matrix | Net Revenue | location_city | UAE map or matrix by city |

**Filters/Slicers**:
- Date Range (default: Current YTD)
- Company Source (Pet Shop, Pet Haus, TPS Cafe, Services)
- Sales Channel
- Division

---

### Tab 2: Channel Performance

**Purpose**: Deep dive into sales channel performance and trends.

**Layout**:
- Row 1: Channel KPI Cards (5 cards: Online, Shop, Affiliate, B2B, Service)
- Row 2: Channel Trend Lines (50%) + Channel Comparison Table (50%)
- Row 3: Online Platform Breakdown + Store Performance Ranking

**Visualizations**:

| Visual | Type | Metrics | Dimensions | Description |
|--------|------|---------|------------|-------------|
| **Channel KPIs** | KPI Cards (5) | Revenue, Growth %, ATV | sales_channel | Key metrics per channel |
| **Channel Trend** | Multi-line Chart | Net Revenue | posting_date, sales_channel | Monthly trend by channel |
| **Channel Matrix** | Table/Matrix | Revenue, Units, ATV, Margin | sales_channel | Sortable comparison table |
| **Online Platform Mix** | Stacked Bar | Net Revenue | online_order_channel, posting_date | Website vs. App breakdown |
| **Store Ranking** | Horizontal Bar | Net Revenue | offline_order_channel | Top/bottom store performers |
| **Affiliate Partner** | Treemap | Net Revenue | affiliate_order_channel | Marketplace contribution |

**Filters/Slicers**:
- Date Range
- Transaction Type (Sale/Refund)
- Company Source
- Location City

---

### Tab 3: Product Performance

**Purpose**: Category and product-level sales analysis.

**Layout**:
- Row 1: Division Selector + Top Metrics
- Row 2: Division Hierarchy Drill-down (60%) + Brand Ranking (40%)
- Row 3: Category Trend + Product Table

**Visualizations**:

| Visual | Type | Metrics | Dimensions | Description |
|--------|------|---------|------------|-------------|
| **Division Summary** | Card Grid | Revenue, Units, Growth | division | 6-7 cards for each pet division |
| **Hierarchy Sunburst** | Sunburst/Drill | Net Revenue | division > item_category > item_subcategory | Interactive drill-down |
| **Brand Ranking** | Bar Chart | Net Revenue | item_brand | Top 15 brands |
| **Category Trend** | Line Chart | Net Revenue | posting_date, item_category | Category growth trends |
| **Product Detail** | Table | item_no_, item_name, Revenue, Units, ATV | - | Searchable product table |
| **Category Mix** | Stacked Area | Revenue % | posting_date, item_category | Category share over time |

**Filters/Slicers**:
- Division (multi-select)
- Item Category
- Item Brand
- Sales Channel
- Date Range

---

### Tab 4: Discount & Promotions Analysis

**Purpose**: Track promotional effectiveness and discount impact on margins.

**Layout**:
- Row 1: Discount KPI Cards
- Row 2: Discount Rate Trend (60%) + Discounted vs. Full Price Comparison (40%)
- Row 3: Top Offers Table + Discount by Category

**Visualizations**:

| Visual | Type | Metrics | Dimensions | Description |
|--------|------|---------|------------|-------------|
| **Total Discount AED** | KPI Card | SUM(discount_amount) | - | Total discounts given |
| **Discount Rate %** | KPI Card | Discount / Gross Revenue | - | Overall discount intensity |
| **Discounted Orders %** | KPI Card | COUNT WHERE has_discount=1 / Total | - | Order discount penetration |
| **Discount Trend** | Combo Chart | Discount Amount (bars), Discount Rate (line) | posting_date | Monthly discount tracking |
| **Discount Status Split** | Pie Chart | Net Revenue | discount_status | Discounted vs. No Discount |
| **Discount by Channel** | Grouped Bar | Discount Amount, Discount Rate | sales_channel | Online vs. Offline discounting |
| **Top Offers** | Table | offline_offer_name/online_offer_no_, Usage Count, Discount Value | - | Most used promotions |
| **Discount by Category** | Horizontal Bar | Discount Rate % | item_category | Which categories are heavily promoted |

**Filters/Slicers**:
- Date Range
- Sales Channel
- Discount Status
- Division
- Offer Code (search)

---

### Tab 5: Geographic & Store Analysis

**Purpose**: Store-level performance and geographic insights.

**Layout**:
- Row 1: City Summary Cards + Overall Map
- Row 2: Store Ranking Table (60%) + Store Trend (40%)
- Row 3: Store Comparison Matrix

**Visualizations**:

| Visual | Type | Metrics | Dimensions | Description |
|--------|------|---------|------------|-------------|
| **City KPIs** | Card Grid (3) | Revenue, Growth, ATV | location_city | Dubai, Abu Dhabi, RAK |
| **UAE Map** | Filled Map | Net Revenue | location_city | Geographic heat map |
| **Store Ranking** | Table | Revenue, Units, ATV, Growth | clc_location_code | Sortable store performance |
| **Store Trend** | Multi-line | Net Revenue | posting_date, clc_location_code | Top 5 stores trend |
| **Store Comparison** | Matrix | Revenue by Month | clc_location_code x Month | Store performance grid |
| **Store Channel Mix** | Stacked Bar | Net Revenue | clc_location_code, sales_channel | How stores contribute by channel |

**Filters/Slicers**:
- City
- Store (multi-select)
- Date Range
- Division
- Item Category

---

### Tab 6: Time-Based Comparisons

**Purpose**: Period-over-period analysis using pre-calculated time flags.

**Layout**:
- Row 1: MTD vs. LMTD vs. LYMTD Comparison
- Row 2: YTD Performance Bridge
- Row 3: Monthly Trend Table

**Visualizations**:

| Visual | Type | Metrics | Dimensions | Description |
|--------|------|---------|------------|-------------|
| **MTD Performance** | KPI + Comparison | Revenue WHERE is_mtd=1 vs. is_lmtd=1 vs. is_lymtd=1 | - | Three-way comparison |
| **YTD vs. LYTD** | Waterfall Chart | Revenue variance | Categories: Start, Growth, Decline, End | Bridge chart for variance |
| **Period Matrix** | Matrix | Revenue | Rows: Division, Cols: MTD/LMTD/LYTD | Multi-period comparison |
| **Rolling 3 Months** | Combo Chart | M_1, M_2, M_3 Revenue | Division | Monthly progression |
| **Year Comparison** | Grouped Bar | Y_1, Y_2, Current Year | Month | Year-over-year monthly bars |
| **Growth Heatmap** | Heatmap | Growth % | Division x Period | Red/green growth matrix |

**Filters/Slicers**:
- Company Source
- Sales Channel
- Division
- Transaction Type

---

### Tab 7: Transaction Detail & Drill-Through

**Purpose**: Line-level detail for ad-hoc analysis and audit.

**Layout**:
- Row 1: Summary KPIs + Search Bar
- Row 2: Transaction Detail Table (full width)
- Row 3: Export Options

**Visualizations**:

| Visual | Type | Columns | Description |
|--------|------|---------|-------------|
| **Transaction Table** | Detail Table | posting_date, document_no_, unified_order_id, customer_name, item_name, invoiced_quantity, sales_amount__actual_, discount_amount, sales_channel, location_code | Fully sortable/filterable detail |
| **Document Search** | Search Box | document_no_, web_order_id | Quick document lookup |
| **Export Button** | Button | All columns | Excel/CSV export |

**Filters/Slicers**:
- All dimension filters available
- Document number search
- Customer search
- Item search

---

## 5. Implementation Prompts

### 5.1 Power BI DAX Measures

```dax
// Net Revenue (Sales Only)
Net Revenue =
CALCULATE(
    SUM(fact_commercial[sales_amount__actual_]),
    fact_commercial[transaction_type] = "Sale"
)

// Gross Revenue
Gross Revenue =
CALCULATE(
    SUM(fact_commercial[sales_amount_gross]),
    fact_commercial[transaction_type] = "Sale"
)

// Total Discount
Total Discount = SUM(fact_commercial[discount_amount])

// Discount Rate %
Discount Rate % =
DIVIDE([Total Discount], [Gross Revenue], 0) * 100

// Gross Margin %
Gross Margin % =
DIVIDE(
    [Net Revenue] - SUM(fact_commercial[cost_amount__actual_]),
    [Net Revenue],
    0
) * 100

// Average Transaction Value
ATV =
DIVIDE(
    [Net Revenue],
    DISTINCTCOUNT(fact_commercial[document_no_]),
    0
)

// MTD Revenue
MTD Revenue =
CALCULATE(
    [Net Revenue],
    fact_commercial[is_mtd] = 1
)

// LYMTD Revenue
LYMTD Revenue =
CALCULATE(
    [Net Revenue],
    fact_commercial[is_lymtd] = 1
)

// YoY Growth %
YoY Growth % =
DIVIDE(
    [MTD Revenue] - [LYMTD Revenue],
    [LYMTD Revenue],
    0
) * 100

// Units Sold
Units Sold =
CALCULATE(
    SUM(fact_commercial[invoiced_quantity]),
    fact_commercial[transaction_type] = "Sale"
)
```

### 5.2 Python/Pandas Analysis Script

```python
import pandas as pd

# Load fact_commercial from BigQuery
query = """
SELECT *
FROM `your_project.gold_layer.fact_commercial`
WHERE posting_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
"""
df = pd.read_gbq(query)

# Key Metrics
net_revenue = df[df['transaction_type'] == 'Sale']['sales_amount__actual_'].sum()
total_discount = df['discount_amount'].sum()
discount_rate = total_discount / df[df['transaction_type'] == 'Sale']['sales_amount_gross'].sum()

# Channel Analysis
channel_summary = df[df['transaction_type'] == 'Sale'].groupby('sales_channel').agg({
    'sales_amount__actual_': 'sum',
    'discount_amount': 'sum',
    'document_no_': 'nunique',
    'invoiced_quantity': 'sum'
}).rename(columns={
    'sales_amount__actual_': 'Revenue',
    'discount_amount': 'Discount',
    'document_no_': 'Transactions',
    'invoiced_quantity': 'Units'
})
channel_summary['ATV'] = channel_summary['Revenue'] / channel_summary['Transactions']
channel_summary['Discount_Rate'] = channel_summary['Discount'] / channel_summary['Revenue']

# Division Performance
division_perf = df[df['transaction_type'] == 'Sale'].groupby('division').agg({
    'sales_amount__actual_': 'sum',
    'cost_amount__actual_': 'sum'
})
division_perf['Margin_%'] = (
    (division_perf['sales_amount__actual_'] - division_perf['cost_amount__actual_']) /
    division_perf['sales_amount__actual_'] * 100
)
```

### 5.3 Looker Studio / Google Data Studio

```yaml
# Data Source Connection
data_source:
  type: BigQuery
  project: your_project
  dataset: gold_layer
  table: fact_commercial

# Calculated Fields
calculated_fields:
  - name: Net Revenue
    formula: |
      CASE
        WHEN transaction_type = 'Sale' THEN sales_amount__actual_
        ELSE 0
      END

  - name: Discount Rate
    formula: |
      SUM(discount_amount) / SUM(sales_amount_gross)

  - name: Gross Margin
    formula: |
      (SUM(sales_amount__actual_) - SUM(cost_amount__actual_)) / SUM(sales_amount__actual_)

# Charts Configuration
charts:
  - type: scorecard
    metric: Net Revenue
    comparison: LYMTD Revenue

  - type: time_series
    dimension: posting_date
    metrics: [Net Revenue, Total Discount]
    breakdown: sales_channel

  - type: geo_map
    geo_dimension: location_city
    color_metric: Net Revenue
```

### 5.4 Tableau Calculated Fields

```tableau
// Net Revenue
IF [Transaction Type] = 'Sale' THEN [Sales Amount Actual] ELSE 0 END

// Gross Margin %
(SUM([Net Revenue]) - SUM([Cost Amount Actual])) / SUM([Net Revenue])

// Discount Penetration
COUNTD(IF [Has Discount] = 1 THEN [Document No] END) / COUNTD([Document No])

// YoY Growth
(SUM(IF [Is Mtd] = 1 THEN [Net Revenue] END) -
 SUM(IF [Is Lymtd] = 1 THEN [Net Revenue] END)) /
SUM(IF [Is Lymtd] = 1 THEN [Net Revenue] END)
```

---

## 6. Best Practices & Recommendations

### 6.1 Performance Optimization

1. **Pre-aggregate for dashboards**: Create summary tables for common aggregations (daily/weekly by channel, division)
2. **Partition by posting_date**: Ensure BigQuery partitioning for efficient date filtering
3. **Use time flags**: Leverage pre-calculated is_mtd, is_ytd flags instead of runtime date calculations
4. **Limit detail tables**: Cap row counts in transaction detail views

### 6.2 Data Quality Checks

1. **Monitor exclusion filters**: Document why specific documents are excluded (e.g., PSI/2021/01307)
2. **Validate discount calculations**: Ensure online_discount_amount VAT adjustment is correct
3. **Check unified_customer_id coverage**: Track what percentage uses phone vs. source_no_
4. **Reconcile to source**: Validate fact_commercial totals against ERP GL entries

### 6.3 Suggested Enhancements

1. **Add margin analysis**: Include unit cost for more granular margin calculations
2. **Customer cohort fields**: Add customer first purchase date for cohort analysis
3. **Inventory flags**: Link to inventory models for stock availability context
4. **Campaign attribution**: Enhanced promotion tracking with campaign codes

---

## 7. Related Models

| Model | Relationship | Use Case |
|-------|--------------|----------|
| `dim_customers` | unified_customer_id | Customer 360 analysis, RFM segmentation |
| `dim_items` | item_no_ | Extended product attributes |
| `dim_date` | posting_date | Calendar hierarchies, fiscal periods |
| `fact_orders` | unified_order_id | Order-level aggregations |
| `int_orders` | unified_order_id | Customer journey, retention analysis |
| `fct_budget` | dimension keys | Budget vs. actual comparisons |

---

*Document generated: 2024 | Model Version: fact_commercial v1.0*
*Last Updated: Based on current model analysis*
