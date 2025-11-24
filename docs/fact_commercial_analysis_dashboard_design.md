# FACT_COMMERCIAL: dbt Logic Review & Multi-Tab Dashboard Design

---

## TASK A: ANALYZE FACT_COMMERCIAL LOGIC (DBT PERSPECTIVE)

---

### A. Model Summary

| Attribute | Description |
|-----------|-------------|
| **Grain** | 1 row per **item ledger entry** (transaction line level) - each row represents a single item sold/refunded in a document |
| **Business Entity** | Commercial sales transactions from Pet Shop and Pet Haus entities |
| **Primary Key** | `item_ledger_entry_no_` (unique per transaction line) |
| **Natural Key** | `document_no_` + `item_no_` + `posting_date` |
| **Main Foreign Keys** | `source_no_` (customer), `item_no_` (product), `location_code` (store), `unified_customer_id`, `unified_order_id` |

**Data Lineage:**
```
stg_value_entry → int_value_entry → int_order_lines → int_commercial → fact_commercial
```

---

### B. Source & Join Logic

#### Source Tables Used

| Source Model | Join Type | Join Key(s) | Purpose |
|--------------|-----------|-------------|---------|
| `stg_value_entry` | Base | - | Core transaction data from ERP |
| `int_inbound_sales_header` | LEFT | `document_no_`, `company_source = 'Pet Shop'` | Online order details (web_order_id, payment gateway, order type) |
| `int_erp_customer` | LEFT | `source_no_ = no_`, `company_source = 'Pet Shop'` | Customer information (name, phone, loyalty ID) |
| `int_items` | LEFT | `item_no_` | Product master data (category, brand, division) |
| `inbound_sales_line_dedup` (CTE) | LEFT | `document_no_`, `item_no_` | Online discount amounts and coupon codes |
| `int_discount_ledger_entry` | LEFT | `item_ledger_entry_no_` | Offline discount and offer information |

#### Critical Filters Applied

| Filter Level | Condition | Purpose |
|--------------|-----------|---------|
| `int_order_lines` | `item_ledger_entry_type = 'Sale'` | Only sales transactions (excludes purchases, adjustments, transfers) |
| `int_order_lines` | `source_code IN ('BACKOFFICE', 'SALES')` | Only legitimate sales sources |
| `int_order_lines` | `document_type NOT IN ('Sales Shipment', 'Sales Return Receipt')` | Excludes non-revenue documents |
| `int_order_lines` | `dimension_code = 'PROFITCENTER'` | Only profit center dimension entries |
| `fact_commercial` | `document_no_ NOT IN ('PSI/2021/01307', 'PSI/2023/00937', 'INV00528612')` | Excludes specific erroneous documents |
| `fact_commercial` | Excludes Pet Shop + Mobile/Shop Grooming combo | Business rule exclusion |

---

### C. Important Measures & Columns

#### Key Numeric Columns (Facts)

| Column | Type | Description | Calculation Logic |
|--------|------|-------------|-------------------|
| `invoiced_quantity` | INTEGER | Units sold (negative for refunds) | Direct from value entry |
| `sales_amount_gross` | DECIMAL | Revenue before discount (AED) | `sales_amount__actual_ + ABS(discount_amount)` for sales; `sales_amount__actual_ - ABS(discount_amount)` for refunds |
| `sales_amount__actual_` | DECIMAL | Net revenue after discount (AED) | Direct from value entry |
| `cost_amount__actual_` | DECIMAL | Cost of goods sold (AED) | Direct from value entry (usually negative) |
| `discount_amount` | DECIMAL | Total discount applied (AED) | `COALESCE(online_discount_amount, 0)` or `COALESCE(offline_discount_amount, 0)` based on channel |
| `online_discount_amount` | DECIMAL | Online channel discount | `(-1 * inbound_sales_line.discount_amount) / 1.05` (VAT adjusted) |
| `offline_discount_amount` | DECIMAL | Offline channel discount | Direct from value entry |
| `has_discount` | INTEGER | Discount flag (0/1) | 1 if discount_amount != 0 |

#### Derived Metrics (for dashboards)

| Metric | Formula | Columns Used |
|--------|---------|--------------|
| **Net Revenue** | `SUM(sales_amount__actual_)` | `sales_amount__actual_` |
| **Gross Revenue** | `SUM(sales_amount_gross)` | `sales_amount_gross` |
| **Margin (AED)** | `SUM(sales_amount__actual_) + SUM(cost_amount__actual_)` | `sales_amount__actual_`, `cost_amount__actual_` |
| **Margin %** | `Margin / Net Revenue * 100` | Calculated |
| **Total Discount** | `SUM(discount_amount)` | `discount_amount` |
| **Discount %** | `Total Discount / Gross Revenue * 100` | Calculated |
| **Order Count** | `COUNT(DISTINCT unified_order_id)` | `unified_order_id` |
| **AOV** | `Net Revenue / Order Count` | Calculated |
| **Customer Count** | `COUNT(DISTINCT unified_customer_id)` | `unified_customer_id` |
| **Units Sold** | `SUM(invoiced_quantity)` | `invoiced_quantity` |

#### Key Dimensional Columns

| Column | Values | Business Use |
|--------|--------|--------------|
| `sales_channel` | Online, Shop, Affiliate, B2B, Service | Primary channel segmentation |
| `transaction_type` | Sale, Refund, Other | Revenue vs returns analysis |
| `company_source` | Pet Shop, Pethaus | Multi-entity analysis |
| `location_code` | DIP, FZN, REM, UMSQ, WSL, etc. | Store-level performance |
| `location_city` | Dubai, Abu Dhabi, Ras Al Khaimah | Regional analysis |
| `item_category` | Category hierarchy | Product performance |
| `division` | Division grouping | High-level product segmentation |
| `item_brand` | Brand names | Brand performance |
| `online_order_channel` | website, Android, iOS, CRM | Digital platform analysis |
| `paymentgateway` | creditCard, cash, COD, Tabby, etc. | Payment method analysis |
| `discount_status` | Discounted, No Discount | Promotion impact analysis |
| `is_mtd`, `is_ytd`, etc. | 0/1 flags | Time-based filtering |

---

### D. Data Quality / Modeling Comments

#### Potential Risks

| Risk | Description | Impact |
|------|-------------|--------|
| **Grain Ambiguity** | Item ledger entry level may create duplicates when same item appears multiple times in one order | Overcounting if not careful with aggregations |
| **Refund Handling** | Refunds have negative `invoiced_quantity` and `sales_amount__actual_` | Net calculations correct, but separate reporting needed |
| **NULL Customer IDs** | Offline transactions may lack `std_phone_no_`, defaulting to `source_no_` | Customer deduplication challenges |
| **Discount Attribution** | Online/offline discount logic differs (VAT adjustment for online) | Ensure consistency in discount analysis |
| **Time Period Flags** | Calculated dynamically based on `CURRENT_DATE()` | Historical reports will show different results over time |

#### Recommended Improvements

1. **Add surrogate key**: Create `fact_commercial_sk` as `CONCAT(item_ledger_entry_no_, '_', company_source)` for explicit uniqueness
2. **Standardize currency**: Add explicit currency column (currently all AED assumed)
3. **Add calculated margin columns**: Pre-calculate `margin_amount` and `margin_pct` for performance
4. **Implement snapshot time flags**: Store `snapshot_date` alongside period flags for reproducible reporting
5. **Add data quality column**: Flag rows with potential issues (missing customer, negative gross, etc.)

---

## TASK B: MULTI-TAB COMMERCIAL DASHBOARD DESIGN

---

### Dashboard Architecture Overview

```
+------------------------------------------------------------------+
|                    COMMERCIAL ANALYTICS DASHBOARD                 |
|------------------------------------------------------------------|
| [Overview] [Revenue] [Customers] [Products] [Channels] [Discounts]|
+------------------------------------------------------------------+
|  Global Filters: Date Range | Company | Sales Channel | City     |
+------------------------------------------------------------------+
```

---

## TAB 1: EXECUTIVE OVERVIEW

### 1) Tab Name
**Executive Overview**

### 2) Business Purpose
- Provide C-level snapshot of overall commercial performance
- Enable quick identification of trends and anomalies
- Compare current performance vs prior periods (MTD, YTD, LY)
- Monitor key health indicators across all dimensions

### 3) Primary KPIs (Top Cards)

| # | KPI Name | Definition | Columns | Aggregation |
|---|----------|------------|---------|-------------|
| 1 | **Net Revenue** | Total revenue after discounts | `sales_amount__actual_` | `SUM()` |
| 2 | **Gross Margin %** | Profit as percentage of revenue | `sales_amount__actual_`, `cost_amount__actual_` | `(SUM(actual) + SUM(cost)) / SUM(actual) * 100` |
| 3 | **Total Orders** | Unique order count | `unified_order_id` | `COUNT(DISTINCT)` WHERE `transaction_type = 'Sale'` |
| 4 | **Average Order Value** | Revenue per order | Calculated | `Net Revenue / Total Orders` |
| 5 | **Active Customers** | Unique buyers | `unified_customer_id` | `COUNT(DISTINCT)` WHERE `transaction_type = 'Sale'` |
| 6 | **Units Sold** | Total quantity | `invoiced_quantity` | `SUM()` WHERE `transaction_type = 'Sale'` |
| 7 | **Refund Rate** | Refunds as % of gross sales | `transaction_type`, `sales_amount__actual_` | `ABS(SUM WHERE Refund) / SUM WHERE Sale * 100` |
| 8 | **Discount Rate** | Discounts as % of gross | `discount_amount`, `sales_amount_gross` | `SUM(discount) / SUM(gross) * 100` |

### 4) Filters / Slicers

| Filter | Column | Type | Default |
|--------|--------|------|---------|
| Date Range | `posting_date` | Date picker | Current MTD |
| Company | `company_source` | Multi-select | All |
| Sales Channel | `sales_channel` | Multi-select | All |
| City | `location_city` | Multi-select | All |
| Transaction Type | `transaction_type` | Single select | Sale |

### 5) Main Visuals (Top Section)

| Visual | Type | X-Axis | Y-Axis | Purpose |
|--------|------|--------|--------|---------|
| **Revenue Trend** | Line + Area | `posting_date` (daily/weekly/monthly) | `SUM(sales_amount__actual_)` | Track revenue trajectory |
| **Channel Mix** | Donut Chart | `sales_channel` | `SUM(sales_amount__actual_)` | Channel contribution |
| **Period Comparison** | KPI Cards w/ Sparklines | Time flags (`is_mtd`, `is_lymtd`) | Multiple metrics | MTD vs LYMTD comparison |
| **Top 5 Stores** | Horizontal Bar | `location_code` | `SUM(sales_amount__actual_)` | Store leaderboard |

### 6) Detail Visuals (Bottom Section)

| Visual | Type | Dimensions | Metrics | Purpose |
|--------|------|------------|---------|---------|
| **Daily Performance Table** | Matrix | `posting_date` (rows), Metrics (cols) | Revenue, Orders, AOV, Margin% | Daily drill-down |
| **Channel Performance** | Clustered Bar | `sales_channel` | Revenue, Orders, AOV | Compare channel metrics |
| **Geographic Heatmap** | Filled Map | `location_city` | `SUM(sales_amount__actual_)` | Regional performance |

### 7) Interactivity
- Cross-filter all visuals on click
- Drill-through to Channel tab on channel click
- Drill-through to Store detail on location click
- Tooltip showing Orders, Units, Margin on hover

---

## TAB 2: REVENUE & MARGIN ANALYSIS

### 1) Tab Name
**Revenue & Margin**

### 2) Business Purpose
- Deep-dive into revenue composition and profitability
- Understand gross-to-net revenue waterfall
- Analyze margin trends and drivers
- Identify high/low margin segments

### 3) Primary KPIs

| # | KPI Name | Definition | Columns | Aggregation |
|---|----------|------------|---------|-------------|
| 1 | **Gross Revenue** | Revenue before discounts | `sales_amount_gross` | `SUM()` |
| 2 | **Total Discounts** | All discounts given | `discount_amount` | `SUM()` |
| 3 | **Net Revenue** | After-discount revenue | `sales_amount__actual_` | `SUM()` |
| 4 | **COGS** | Cost of goods sold | `cost_amount__actual_` | `ABS(SUM())` |
| 5 | **Gross Margin (AED)** | Profit in AED | Calculated | `Net Revenue - COGS` |
| 6 | **Gross Margin %** | Profit percentage | Calculated | `Margin / Net Revenue * 100` |
| 7 | **Revenue per Unit** | Avg selling price | Calculated | `Net Revenue / Units Sold` |
| 8 | **Margin per Unit** | Profit per item | Calculated | `Margin / Units Sold` |

### 4) Filters / Slicers

| Filter | Column | Type | Default |
|--------|--------|------|---------|
| Date Range | `posting_date` | Date picker | Current MTD |
| Division | `division` | Multi-select | All |
| Item Category | `item_category` | Multi-select | All |
| Brand | `item_brand` | Multi-select | All |
| Sales Channel | `sales_channel` | Multi-select | All |
| Transaction Type | `transaction_type` | Single select | Sale |

### 5) Main Visuals (Top Section)

| Visual | Type | Configuration | Purpose |
|--------|------|---------------|---------|
| **Revenue Waterfall** | Waterfall Chart | Gross → (-Discount) → Net → (-COGS) → Margin | Show revenue decomposition |
| **Margin % Trend** | Line Chart | `posting_date` vs Margin % | Track profitability over time |
| **Revenue vs Margin Scatter** | Scatter Plot | X: Revenue, Y: Margin %, Bubble: Orders, Color: Channel | Identify profitable segments |
| **YoY Revenue Comparison** | Combo Chart | Current vs LY revenue by month | Year-over-year trends |

### 6) Detail Visuals (Bottom Section)

| Visual | Type | Rows | Metrics |
|--------|------|------|---------|
| **Profitability Matrix** | Matrix/Table | `division` → `item_category` | Gross Rev, Discount, Net Rev, COGS, Margin, Margin% |
| **Channel Profitability** | Stacked Bar | `sales_channel` | Margin contribution |
| **Brand Margin Analysis** | Bar Chart | `item_brand` (Top 15) | Margin %, Revenue (dual axis) |

### 7) Interactivity
- Waterfall drill-down by channel on click
- Scatter plot tooltip with category/brand details
- Matrix expand/collapse by hierarchy
- Cross-highlight between channel and category views

---

## TAB 3: CUSTOMER PERFORMANCE

### 1) Tab Name
**Customer Performance**

### 2) Business Purpose
- Analyze customer base health and composition
- Track customer acquisition and value metrics
- Identify customer segments by behavior
- Monitor loyalty program effectiveness

### 3) Primary KPIs

| # | KPI Name | Definition | Columns | Aggregation |
|---|----------|------------|---------|-------------|
| 1 | **Total Customers** | Unique buyers | `unified_customer_id` | `COUNT(DISTINCT)` |
| 2 | **Verified Customers** | With verified identity | `customer_identity_status` | `COUNT(DISTINCT)` WHERE status = 'Verified' |
| 3 | **Loyalty Members** | In loyalty program | `loyality_member_id` | `COUNT(DISTINCT)` WHERE NOT NULL |
| 4 | **Revenue per Customer** | Customer value | Calculated | `Net Revenue / Total Customers` |
| 5 | **Orders per Customer** | Purchase frequency | Calculated | `Total Orders / Total Customers` |
| 6 | **Units per Customer** | Basket depth | Calculated | `Units Sold / Total Customers` |
| 7 | **Duplicate Rate** | % flagged duplicates | `duplicate_flag` | `COUNT WHERE 'Yes' / Total * 100` |
| 8 | **Customer Concentration** | Top 10% revenue share | Calculated | Pareto analysis |

### 4) Filters / Slicers

| Filter | Column | Type | Default |
|--------|--------|------|---------|
| Date Range | `posting_date` | Date picker | Current YTD |
| Customer Status | `customer_identity_status` | Multi-select | All |
| Loyalty Status | `loyality_member_id` (NULL/NOT NULL) | Toggle | All |
| Sales Channel | `sales_channel` | Multi-select | All |
| City | `location_city` | Multi-select | All |
| Duplicate Flag | `duplicate_flag` | Single select | No |

### 5) Main Visuals (Top Section)

| Visual | Type | Configuration | Purpose |
|--------|------|---------------|---------|
| **Customer Growth Trend** | Line Chart | `posting_date` vs Cumulative New Customers | Track acquisition |
| **Customer Segmentation** | Pie Chart | By `sales_channel` | Channel preference |
| **Loyalty vs Non-Loyalty** | Combo Bar | Revenue and Orders by Loyalty Status | Loyalty impact |
| **Customer Value Distribution** | Histogram | Revenue per Customer buckets | Value segmentation |

### 6) Detail Visuals (Bottom Section)

| Visual | Type | Configuration | Purpose |
|--------|------|---------------|---------|
| **Top Customers Table** | Table | `unified_customer_id`, `customer_name`, Revenue, Orders, AOV, Last Purchase | High-value customer view |
| **Customer by City** | Map/Bar | `location_city` vs Customer Count | Geographic distribution |
| **Verification Status** | Stacked Bar | `customer_identity_status` by Channel | Data quality by channel |
| **Purchase Frequency** | Bar Chart | Orders per customer buckets | Repeat purchase analysis |

### 7) Interactivity
- Click on segment to filter Top Customers table
- Drill-through to customer transaction detail
- Loyalty toggle shows side-by-side comparison
- Export top customer list

---

## TAB 4: PRODUCT & CATEGORY PERFORMANCE

### 1) Tab Name
**Product Performance**

### 2) Business Purpose
- Analyze product portfolio performance
- Identify best/worst performing categories and items
- Track brand and division contributions
- Optimize product mix decisions

### 3) Primary KPIs

| # | KPI Name | Definition | Columns | Aggregation |
|---|----------|------------|---------|-------------|
| 1 | **Active SKUs** | Products sold | `item_no_` | `COUNT(DISTINCT)` |
| 2 | **Active Categories** | Categories with sales | `item_category` | `COUNT(DISTINCT)` |
| 3 | **Active Brands** | Brands with sales | `item_brand` | `COUNT(DISTINCT)` |
| 4 | **Revenue per SKU** | Productivity | Calculated | `Net Revenue / Active SKUs` |
| 5 | **Top Category Revenue** | #1 category sales | `item_category` | `MAX(SUM by category)` |
| 6 | **Top Brand Revenue** | #1 brand sales | `item_brand` | `MAX(SUM by brand)` |
| 7 | **Avg Units per Order** | Basket size | Calculated | `Units / Orders` |
| 8 | **Category Concentration** | Top 5 category % | Calculated | Concentration ratio |

### 4) Filters / Slicers

| Filter | Column | Type | Default |
|--------|--------|------|---------|
| Date Range | `posting_date` | Date picker | Current MTD |
| Division | `division` | Multi-select | All |
| Category | `item_category` | Multi-select | All |
| Subcategory | `item_subcategory` | Multi-select | All |
| Brand | `item_brand` | Multi-select | All |
| Item Type | `item_type` | Multi-select | All |
| Sales Channel | `sales_channel` | Multi-select | All |

### 5) Main Visuals (Top Section)

| Visual | Type | Configuration | Purpose |
|--------|------|---------------|---------|
| **Division Treemap** | Treemap | `division` → `item_category` sized by Revenue | Portfolio overview |
| **Category Revenue Trend** | Line Chart | Top 5 categories over time | Trend analysis |
| **Brand Performance Scatter** | Scatter | X: Revenue, Y: Margin%, Bubble: Units | Brand positioning |
| **Category Mix Change** | 100% Stacked Area | Category % over time | Mix evolution |

### 6) Detail Visuals (Bottom Section)

| Visual | Type | Rows | Metrics |
|--------|------|------|---------|
| **Product Hierarchy Table** | Matrix | `division` → `item_category` → `item_subcategory` | Revenue, Units, Margin%, Orders |
| **Top 20 SKUs** | Table | `item_no_`, `item_name` | Revenue, Units, AOV, Margin |
| **Bottom 20 SKUs** | Table | `item_no_`, `item_name` (sorted ASC) | Revenue, Units, Margin |
| **Category by Channel** | Heatmap | `item_category` vs `sales_channel` | Revenue intensity |

### 7) Interactivity
- Treemap drill-down through hierarchy
- Click category to filter all visuals
- Sort tables by any metric
- Drill-through to SKU detail view

---

## TAB 5: CHANNEL & LOCATION PERFORMANCE

### 1) Tab Name
**Channels & Locations**

### 2) Business Purpose
- Compare performance across sales channels
- Analyze store/location-level metrics
- Understand online platform mix
- Evaluate geographic expansion opportunities

### 3) Primary KPIs

| # | KPI Name | Definition | Columns | Aggregation |
|---|----------|------------|---------|-------------|
| 1 | **Online Revenue %** | Digital share | `sales_channel` | `SUM WHERE Online / Total * 100` |
| 2 | **Shop Revenue %** | Retail share | `sales_channel` | `SUM WHERE Shop / Total * 100` |
| 3 | **Affiliate Revenue %** | Marketplace share | `sales_channel` | `SUM WHERE Affiliate / Total * 100` |
| 4 | **B2B Revenue %** | Business share | `sales_channel` | `SUM WHERE B2B / Total * 100` |
| 5 | **Top Store Revenue** | #1 location | `location_code` | `MAX(SUM by location)` |
| 6 | **Store Count** | Active locations | `location_code` | `COUNT(DISTINCT)` |
| 7 | **Revenue per Store** | Store productivity | Calculated | `Total Revenue / Store Count` |
| 8 | **Dubai Revenue Share** | Capital concentration | `location_city` | `SUM WHERE Dubai / Total * 100` |

### 4) Filters / Slicers

| Filter | Column | Type | Default |
|--------|--------|------|---------|
| Date Range | `posting_date` | Date picker | Current MTD |
| Sales Channel | `sales_channel` | Multi-select | All |
| Channel Detail | `sales_channel_detail` | Multi-select | All |
| Location | `location_code` | Multi-select | All |
| City | `location_city` | Multi-select | All |
| Online Platform | `online_order_channel` | Multi-select | All (Online only) |
| Affiliate Partner | `affiliate_order_channel` | Multi-select | All (Affiliate only) |

### 5) Main Visuals (Top Section)

| Visual | Type | Configuration | Purpose |
|--------|------|---------------|---------|
| **Channel Revenue Comparison** | Column Chart | `sales_channel` vs Revenue, with MoM% | Channel performance |
| **Channel Trend** | 100% Stacked Area | `sales_channel` % over time | Channel mix shift |
| **Store Map** | Filled Map | `location_city` bubbles sized by Revenue | Geographic view |
| **Online Platform Mix** | Donut | `online_order_channel` | Digital channel breakdown |

### 6) Detail Visuals (Bottom Section)

| Visual | Type | Configuration | Purpose |
|--------|------|---------------|---------|
| **Store Performance Table** | Table | `location_code`, City, Channel | Revenue, Orders, AOV, Margin%, Units |
| **Affiliate Partner Ranking** | Horizontal Bar | `affiliate_order_channel` | Revenue by marketplace |
| **Online vs Offline KPIs** | Comparison Cards | Split by channel type | Side-by-side metrics |
| **Channel x City Matrix** | Matrix | `sales_channel` rows, `location_city` cols | Revenue heatmap |

### 7) Interactivity
- Click channel to filter store table
- Map click filters to city
- Drill-through to store detail page
- Toggle between Revenue/Orders/Margin view

---

## TAB 6: DISCOUNTS & PROMOTIONS

### 1) Tab Name
**Discounts & Promotions**

### 2) Business Purpose
- Track discount effectiveness and costs
- Analyze promotional campaign performance
- Compare discount strategies by channel
- Optimize promotional spending

### 3) Primary KPIs

| # | KPI Name | Definition | Columns | Aggregation |
|---|----------|------------|---------|-------------|
| 1 | **Total Discount (AED)** | All discounts given | `discount_amount` | `SUM()` |
| 2 | **Discount Rate %** | Discount as % of gross | `discount_amount`, `sales_amount_gross` | `SUM(discount) / SUM(gross) * 100` |
| 3 | **Discounted Orders %** | Orders with discount | `has_discount`, `unified_order_id` | `COUNTD WHERE has=1 / Total * 100` |
| 4 | **Online Discount Total** | Digital discount cost | `online_discount_amount` | `SUM()` |
| 5 | **Offline Discount Total** | Store discount cost | `offline_discount_amount` | `SUM()` |
| 6 | **Avg Discount per Order** | Discount depth | Calculated | `Total Discount / Discounted Orders` |
| 7 | **Promo Orders** | Orders with promo code | `online_offer_no_` or `offline_offer_no_` | `COUNT(DISTINCT)` WHERE NOT NULL |
| 8 | **Promo Conversion Lift** | Revenue from promos | Calculated | Revenue WHERE has_discount=1 |

### 4) Filters / Slicers

| Filter | Column | Type | Default |
|--------|--------|------|---------|
| Date Range | `posting_date` | Date picker | Current MTD |
| Discount Status | `discount_status` | Single select | All |
| Sales Channel | `sales_channel` | Multi-select | All |
| Online Promo Code | `online_offer_no_` | Multi-select | All |
| Offline Offer | `offline_offer_no_` | Multi-select | All |
| Offline Offer Name | `offline_offer_name` | Multi-select | All |
| Category | `item_category` | Multi-select | All |

### 5) Main Visuals (Top Section)

| Visual | Type | Configuration | Purpose |
|--------|------|---------------|---------|
| **Discount Rate Trend** | Line Chart | `posting_date` vs Discount % | Track discount intensity |
| **Discounted vs Full Price** | Waterfall | Gross → Discount → Net | Impact visualization |
| **Channel Discount Comparison** | Grouped Bar | `sales_channel` vs Discount Rate % | Compare strategies |
| **Discount Distribution** | Histogram | Discount amount buckets | Distribution analysis |

### 6) Detail Visuals (Bottom Section)

| Visual | Type | Configuration | Purpose |
|--------|------|---------------|---------|
| **Promo Code Performance** | Table | `online_offer_no_` / `offline_offer_name` | Orders, Revenue, Discount, AOV |
| **Category Discount Analysis** | Bar Chart | `item_category` vs Discount % | Category discounting |
| **Online vs Offline Discount** | Comparison Cards | Split view | Channel discount comparison |
| **Discount by Day of Week** | Column Chart | Day name vs Discount Rate | Timing patterns |

### 7) Interactivity
- Click on promo code to filter all visuals
- Toggle between absolute discount and percentage views
- Drill-through to promo detail
- Export promo performance report

---

## TAB 7: OPERATIONAL / ORDER FLOW

### 1) Tab Name
**Order Operations**

### 2) Business Purpose
- Monitor order processing and fulfillment
- Analyze payment method mix
- Track refund trends and root causes
- Identify operational patterns

### 3) Primary KPIs

| # | KPI Name | Definition | Columns | Aggregation |
|---|----------|------------|---------|-------------|
| 1 | **Total Orders** | All orders | `unified_order_id` | `COUNT(DISTINCT)` WHERE Sale |
| 2 | **Total Refunds** | Refund count | `unified_refund_id` | `COUNT(DISTINCT)` |
| 3 | **Refund Rate %** | Refunds / Orders | Calculated | `Refunds / (Orders + Refunds) * 100` |
| 4 | **Refund Value %** | Refund $ / Sales $ | `transaction_type`, `sales_amount__actual_` | `ABS(SUM Refund) / SUM Sale * 100` |
| 5 | **COD Orders %** | Cash on delivery share | `paymentmethodcode` | `COUNTD WHERE COD / Total * 100` |
| 6 | **Prepaid Orders %** | Prepaid share | `paymentmethodcode` | `COUNTD WHERE PREPAID / Total * 100` |
| 7 | **Express Orders %** | Fast delivery share | `order_type` | `COUNTD WHERE EXPRESS / Total * 100` |
| 8 | **Avg Items per Order** | Order size | Calculated | `Total Lines / Total Orders` |

### 4) Filters / Slicers

| Filter | Column | Type | Default |
|--------|--------|------|---------|
| Date Range | `posting_date` | Date picker | Current MTD |
| Transaction Type | `transaction_type` | Multi-select | All |
| Document Type | `document_type` | Multi-select | All |
| Order Type | `order_type` | Multi-select | All |
| Payment Method | `paymentmethodcode` | Multi-select | All |
| Payment Gateway | `paymentgateway` | Multi-select | All |
| Sales Channel | `sales_channel` | Multi-select | All |

### 5) Main Visuals (Top Section)

| Visual | Type | Configuration | Purpose |
|--------|------|---------------|---------|
| **Order Volume Trend** | Line Chart | `posting_date` vs Order Count | Volume tracking |
| **Sales vs Refunds** | Stacked Area | `transaction_type` over time | Refund trend |
| **Payment Method Mix** | Donut Chart | `paymentmethodcode` | Payment preferences |
| **Order Type Distribution** | Pie Chart | `order_type` | Delivery type mix |

### 6) Detail Visuals (Bottom Section)

| Visual | Type | Configuration | Purpose |
|--------|------|---------------|---------|
| **Payment Gateway Analysis** | Table | `paymentgateway` | Orders, Revenue, AOV, Refund Rate |
| **Refund by Category** | Bar Chart | `item_category` | Refund Count, Refund Value |
| **Order by Hour/Day** | Heatmap | Day of Week vs Hour | Peak patterns |
| **Document Type Breakdown** | Table | `document_type` | Count, Value, Avg Value |

### 7) Interactivity
- Click payment method to filter all visuals
- Drill-through to refund detail
- Toggle between count and value views
- Export operational report

---

## CROSS-CUTTING DESIGN ELEMENTS

### Global Filters (Persistent Across All Tabs)

| Filter | Column | Placement |
|--------|--------|-----------|
| **Date Range** | `posting_date` | Top bar |
| **Company** | `company_source` | Top bar |
| **Quick Period** | `is_mtd`, `is_ytd`, etc. | Button group |

### Color Scheme Recommendations

| Element | Color | Hex |
|---------|-------|-----|
| Online | Blue | #2563EB |
| Shop | Green | #16A34A |
| Affiliate | Orange | #EA580C |
| B2B | Purple | #9333EA |
| Service | Teal | #0D9488 |
| Positive Variance | Green | #22C55E |
| Negative Variance | Red | #EF4444 |

### Tooltip Standards

All visuals should include tooltips showing:
- Primary metric value
- Comparison to prior period (% change)
- Related supporting metrics

### Mobile Responsiveness

- Overview tab: 4 KPIs → 2x2 grid
- Charts: Stack vertically on mobile
- Tables: Horizontal scroll enabled
- Filters: Collapsible filter panel

---

## IMPLEMENTATION NOTES

### Semantic Layer Measures (for BI tools)

```sql
-- Core Measures
Net_Revenue = SUM([sales_amount__actual_])
Gross_Revenue = SUM([sales_amount_gross])
Total_COGS = ABS(SUM([cost_amount__actual_]))
Total_Discount = SUM([discount_amount])
Total_Units = SUM([invoiced_quantity])
Order_Count = DISTINCTCOUNT([unified_order_id])
Customer_Count = DISTINCTCOUNT([unified_customer_id])

-- Calculated Measures
Margin_AED = [Net_Revenue] - [Total_COGS]
Margin_Pct = DIVIDE([Margin_AED], [Net_Revenue], 0) * 100
Discount_Pct = DIVIDE([Total_Discount], [Gross_Revenue], 0) * 100
AOV = DIVIDE([Net_Revenue], [Order_Count], 0)
Revenue_Per_Customer = DIVIDE([Net_Revenue], [Customer_Count], 0)
Units_Per_Order = DIVIDE([Total_Units], [Order_Count], 0)

-- Time Intelligence (using period flags)
MTD_Revenue = CALCULATE([Net_Revenue], [is_mtd] = 1)
LYMTD_Revenue = CALCULATE([Net_Revenue], [is_lymtd] = 1)
YoY_Growth = DIVIDE([MTD_Revenue] - [LYMTD_Revenue], [LYMTD_Revenue], 0) * 100
```

### Performance Optimization

1. **Pre-aggregate time flags**: Use the `is_mtd`, `is_ytd` flags instead of date calculations
2. **Index recommendations**: Index on `posting_date`, `sales_channel`, `unified_customer_id`
3. **Partition strategy**: Consider partitioning by `posting_date` (monthly)
4. **Incremental refresh**: Refresh only recent data (current month + 1)

---

*Document generated: Based on fact_commercial analysis*
*Data Model Version: int_order_lines → int_commercial → fact_commercial*
