# fact_commercial Model Evolution Timeline

**Model:** `models/3_fct/fact_commercial.sql`
**Total Commits:** 5 (for current version) + 2 (for predecessor model)
**Development Period:** June 23, 2025 - October 25, 2025 (124 days)
**Primary Developer:** Anmar Abbas DataGo

---

## Overview

The `fact_commercial` model has undergone a **complete architectural redesign** during its lifecycle. The model evolved from an **order-level hyperlocal analysis table** (`fct_commercial.sql` - deleted) to a **line-level ERP value entry fact table** (`fact_commercial.sql` - current), representing a fundamental shift in analytical approach.

---

## Two Distinct Eras

### **Era 1: Order-Level Hyperlocal Analysis (June 23 - July 7, 2025)**
**File:** `fct_commercial.sql` (old naming convention)
**Focus:** Order-level aggregation with hyperlocal delivery analysis
**Status:** **Deleted on July 7, 2025**

### **Era 2: Line-Level ERP Commercial Analytics (August 8, 2025 - Present)**
**File:** `fact_commercial.sql` (new naming convention)
**Focus:** Transaction line-level ERP value entries with commercial metrics
**Status:** **Active**

---

## Era 1: Order-Level Hyperlocal Model (Predecessor)

### 📅 **Commit 1: Initial Creation of fct_commercial.sql**
**Date:** June 23, 2025
**Commit ID:** `dc697b4f2910c98a2fee5aaaaa356a825ec552cc`
**File:** `models/3_fct/fct_commercial.sql`
**Feature Name:** **Hyperlocal Launch Analysis Model**

**What Was Built:**
A comprehensive **order-level** fact table focused on analyzing the impact of the **hyperlocal 60-minute delivery launch** on January 16, 2025.

**Key Features:**
1. **Order Identification**
   - source_no_, unified_order_id, document_no_, web_order_id
   - Order date and time dimensions

2. **Revenue Separation**
   - order_value (Sales Invoice amounts only)
   - refund_amount (Credit Memo amounts only)
   - sales_amount__actual_ (raw total)
   - document_type_2 (Sales Invoice vs. Credit Memo)
   - transaction_type (Sale, Refund, Other)

3. **Hyperlocal Analysis Dimensions** ⭐ (Core Focus)
   - hyperlocal_period (Pre-Launch/Post-Launch based on Jan 16, 2025)
   - delivery_service_type (60-Min Hyperlocal, 4-Hour Express, Standard)
   - service_tier (Express Service vs. Standard Service)
   - hyperlocal_order_flag (Hyperlocal Order vs. Non-Hyperlocal)
   - days_since_hyperlocal_launch

4. **Customer Hyperlocal Segmentation**
   - hyperlocal_customer_segment (New Post-Hyperlocal, Existing Pre-Hyperlocal)
   - hyperlocal_usage_flag (Used Hyperlocal, Never Used Hyperlocal)
   - hyperlocal_customer_detailed_segment (5 segments combining acquisition timing and usage)

5. **Channel & Platform**
   - sales_channel (Online/Shop)
   - store_location, platform (website/Android/iOS)
   - order_type, paymentgateway, paymentmethodcode

6. **Time Dimensions**
   - order_month, order_week, order_year
   - order_month_num, year_month

**Business Purpose:**
Analyze the business impact of the hyperlocal 60-minute delivery service launch, track customer adoption, and measure revenue shift between delivery tiers.

---

### 📅 **Commit 2: Deletion of fct_commercial.sql**
**Date:** July 7, 2025
**Commit ID:** `0db360a8270b5669ce0f5f11eee145e5e73d6449`
**Action:** **File Deleted**
**Feature Name:** **Model Deprecation**

**What Happened:**
The order-level `fct_commercial.sql` model was **deleted** and replaced by a different analytical approach. The hyperlocal analysis was likely moved to `fact_orders` or other specialized models.

**Reason (Inferred):**
- Need for more granular line-level commercial analysis
- Separation of concerns: Orders analysis vs. Commercial/Value Entry analysis
- Naming convention standardization (fct_ → fact_)

---

## Era 2: Line-Level Commercial Model (Current)

### 📅 **Commit 1: New fact_commercial.sql Creation**
**Date:** August 8, 2025
**Commit ID:** `3b3d38bdc99fe0fe92e43e43905a3c7950d7f0fd`
**Lines Added:** 6
**Feature Name:** **Commercial Value Entry Foundation**

**What Was Built:**
A completely new model with a fundamentally different purpose - focusing on **ERP value entry transactions** at the line level.

**Initial Structure:**
```sql
select
*,
DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 4 HOUR) AS report_last_updated_at,
FROM {{ ref('int_commercial') }}
```

**Key Characteristics:**
- **Simple pass-through** from int_commercial model
- **SELECT *** approach (all columns from upstream)
- **Dubai timezone** reporting (UTC+4)
- **Data Source:** int_commercial (ERP value entries, not orders)

**Analytical Shift:**
- **Before:** Order-level aggregation with hyperlocal focus
- **After:** Transaction line-level with ERP commercial metrics

---

### 📅 **Commit 2: Full Column Documentation**
**Date:** October 6, 2025
**Commit ID:** `6f818cd1b3e148fd8cb6995a493932f46e15956d`
**Lines Added:** 113, Lines Removed: 2
**Net Change:** +111 lines
**Feature Name:** **Explicit Column Documentation & Data Quality Filters**

**Major Enhancement:**
Transitioned from `SELECT *` to **explicitly listing and documenting all 90+ fields** with inline comments.

**What Was Added:**

#### **1. Core Identifiers (4 fields)**
- source_no_ (customer ID)
- document_no_ (ERP document number)
- posting_date (transaction date)
- invoiced_quantity (quantity sold/refunded)

#### **2. Sales Channel Information (8 fields)**
- company_source (Petshop vs. Pethaus)
- sales_channel (Online, Shop, Affiliate, B2B, Service)
- sales_channel_sort (1-6 sort order)
- transaction_type (Sale, Refund, Other)
- offline_order_channel (store code for POS)
- source_code (BACKOFFICE, SALES)
- item_ledger_entry_type (Sale)
- document_type (Sales Invoice, Sales Credit Memo)

#### **3. Discount Information (7 fields)**
- discount_status (Discounted, No Discount)
- has_discount (0/1 flag)
- discount_amount (total AED)
- offline_discount_amount, online_discount_amount
- online_offer_no_, offline_offer_no_ (promo codes)

#### **4. Financial Amounts (3 fields)**
- sales_amount_gross (before discount)
- sales_amount__actual_ (after discount)
- cost_amount__actual_ (cost basis)

#### **5. Posting Groups & ERP Dimensions (10 fields)**
- gen__prod__posting_group, gen__bus__posting_group
- source_posting_group, inventory_posting_group
- global_dimension_1_code, global_dimension_2_code
- dimension_code (PROFITCENTER)
- global_dimension_2_code_name
- clc_global_dimension_2_code_name

#### **6. Location Information (3 fields)**
- location_code (DIP, FZN, REM, UMSQ, WSL)
- clc_location_code (calculated)
- location_city (Dubai, Abu Dhabi, Ras Al Khaimah)

#### **7. Unified IDs (4 fields)**
- unified_order_id (web order or doc number)
- unified_refund_id (refund doc number)
- unified_customer_id (phone or source_no_)
- loyality_member_id (loyalty program ID)

#### **8. Online Order Information (5 fields)**
- web_order_id (online order number)
- online_order_channel (website, Android, iOS, CRM)
- order_type (EXPRESS, NORMAL, EXCHANGE)
- paymentgateway (creditCard, cash, COD, Tabby)
- paymentmethodcode (PREPAID, COD)

#### **9. Customer Information (5 fields)**
- customer_name, std_phone_no_, raw_phone_no_
- duplicate_flag (Yes/No)
- customer_identity_status (Verified/Unverified)

#### **10. Item Information (8 fields)**
- item_no_, item_name, item_category, item_subcategory
- item_brand, division
- division_sort_order, item_category_sort_order

#### **11. Time Period Flags (10 fields)** ⭐
- is_mtd (month-to-date)
- is_ytd (year-to-date)
- is_lmtd (last month-to-date)
- is_lymtd (last year month-to-date)
- is_lytd (last year-to-date)
- is_m_1, is_m_2, is_m_3 (last 3 months full)
- is_y_1, is_y_2 (last 2 years full)

**Data Quality Filters Added:**
```sql
WHERE document_no_ NOT IN ('PSI/2021/01307', 'PSI/2023/00937')
```
- Excludes specific problematic documents (likely data quality issues)

**Business Purpose:**
Enable granular commercial analysis at the transaction line level with ERP integration, supporting profitability analysis, discount effectiveness, and channel performance.

---

### 📅 **Commit 3: Grooming Service Filter**
**Date:** October 15, 2025
**Commit ID:** `6cd3a1f3fa88a90bce7000a0a9136448e9a3607c`
**Lines Added:** 11, Lines Removed: 3
**Feature Name:** **Item Type Field & Grooming Exclusion Logic**

**What Was Added:**
1. **item_ledger_entry_no_** - Added ERP entry number for traceability
2. **item_type** - Added item type classification

**Filter Enhancement:**
```sql
WHERE document_no_ NOT IN ('PSI/2021/01307', 'PSI/2023/00937')
  AND (company_source != 'Pet Shop'
       OR clc_global_dimension_2_code_name NOT IN ('Mobile Grooming','Shop Grooming'))
```

**What This Filter Does:**
- Excludes grooming services from Pet Shop (company_source = 'Pet Shop')
- Specifically filters out 'Mobile Grooming' and 'Shop Grooming' transactions
- Keeps grooming services for other company sources (e.g., Pet Haus)

**Testing Comments Added:**
```sql
and document_no_ = 'DIP-DT08-35221'  -- Document-level testing
```

**Business Purpose:**
Remove grooming service transactions from Pet Shop commercial analysis (likely moved to specialized service revenue model or excluded from product sales analysis).

---

### 📅 **Commit 4: Filter Cleanup**
**Date:** October 15, 2025 (same day)
**Commit ID:** `2fc13e895c74390cb57661072c629ee666f6789b`
**Lines Removed:** 7
**Feature Name:** **Testing Filter Removal**

**What Was Removed:**
- Removed single document test filter (`and document_no_ = 'DIP-DT08-35221'`)
- Cleaned up commented-out filters

**Result:**
Production-ready model with only essential data quality filters.

---

### 📅 **Commit 5: Offline Offer Name Addition**
**Date:** October 25, 2025
**Commit ID:** `7b86fa3505af23d0381ff0a5611b657cdc7cdf11`
**Lines Added:** 8
**Feature Name:** **Offline Promotion Naming**

**What Was Added:**
1. **offline_offer_name** - Human-readable name for offline promotions (in addition to offline_offer_no_)

**Additional Testing Filters (Commented):**
```sql
and (posting_date BETWEEN '2025-01-01' AND '2025-09-30'
       OR posting_date BETWEEN '2024-12-01' AND '2024-12-31'
       OR posting_date BETWEEN '2024-01-01' AND '2024-01-31')
```

**Business Purpose:**
Enable better reporting on offline promotions by showing user-friendly promotion names alongside codes. Supports marketing campaign performance analysis.

---

## Summary of Features by Category

### 🏗️ **Core Structure**
- **Aug 8:** Simple SELECT * from int_commercial
- **Oct 6:** Explicit 90+ field documentation with inline comments

### 💰 **Financial & Commercial Metrics**
- **Oct 6:** Sales amounts (gross/actual), cost amounts, discount tracking
- **Oct 6:** Posting groups and ERP dimensions for financial reporting

### 🏪 **Sales Channel Analytics**
- **Oct 6:** Multi-channel support (Online, Shop, Affiliate, B2B, Service)
- **Oct 6:** Sales channel detail, offline/online order channels
- **Oct 15:** Grooming service exclusion for Pet Shop

### 🎁 **Promotion & Discount Tracking**
- **Oct 6:** Discount status, amounts, and offer codes
- **Oct 25:** Offline offer names for better reporting

### 📍 **Location & Geography**
- **Oct 6:** Store locations (DIP, FZN, REM, UMSQ, WSL)
- **Oct 6:** City mapping (Dubai, Abu Dhabi, Ras Al Khaimah)

### 👥 **Customer Integration**
- **Oct 6:** Unified customer IDs, phone standardization
- **Oct 6:** Customer identity status, duplicate flags
- **Oct 6:** Loyalty program integration

### 📦 **Item & Product Information**
- **Oct 6:** Item details (category, subcategory, brand, division)
- **Oct 15:** Item type classification

### 📅 **Time Intelligence**
- **Oct 6:** 10 time period flags (MTD, YTD, LMTD, LYMTD, LYTD, M-1/2/3, Y-1/2)
- Enables easy period-over-period comparisons

### 🔍 **Data Quality**
- **Oct 6:** Document exclusion filter (2 problematic documents)
- **Oct 15:** Grooming service filter for Pet Shop
- **Oct 15:** Testing filters for development/debugging

---

## Architecture Evolution

### **Phase 1: Order-Level Hyperlocal (June 23 - July 7)**
```
Online Orders + Offline Orders → fct_commercial (ORDER LEVEL)
Focus: Hyperlocal delivery impact analysis
Grain: One row per order
```

### **Phase 2: Simple Pass-Through (Aug 8 - Oct 5)**
```
int_commercial (ERP Value Entries) → fact_commercial (SELECT *)
Focus: Line-level commercial transactions
Grain: One row per value entry line
```

### **Phase 3: Documented Commercial Fact (Oct 6 - Present)**
```
int_commercial → fact_commercial (90+ documented fields + filters)
Focus: Commercial analytics with ERP integration
Grain: One row per value entry line
Filters: Exclude grooming, exclude bad documents
```

---

## Key Architectural Decisions

### **1. Fundamental Shift in Analytical Grain**
- **Old Model:** Order-level aggregation (one row per order)
- **New Model:** Transaction line-level (one row per item sold in each transaction)
- **Impact:** Much more granular analysis, better for profitability and margin analysis

### **2. Focus Change**
- **Old Model:** Hyperlocal delivery service adoption and impact
- **New Model:** Commercial performance across all channels and products
- **Reason:** Hyperlocal analysis likely moved to specialized order models

### **3. Data Source Shift**
- **Old Model:** Likely from int_orders or order aggregations
- **New Model:** From int_commercial (ERP value entries)
- **Impact:** Direct ERP integration, financial reconciliation capability

### **4. Documentation Philosophy**
- **Initial:** SELECT * (implicit, relies on upstream)
- **Current:** Explicit field listing with inline documentation
- **Benefit:** Self-documenting, easier to understand and maintain

---

## Current State (as of Oct 25, 2025)

**Lines of Code:** ~127 lines
**Total Fields:** 90+ commercial metrics
**Data Source:** int_commercial (ERP value entries)
**Grain:** Transaction line level
**Materialization:** Not specified (likely table)
**Filters Active:**
- Excludes 2 problematic documents
- Excludes Pet Shop grooming services (Mobile + Shop Grooming)

**Key Characteristics:**
- ✅ Comprehensive inline documentation (every field commented)
- ✅ Multi-company support (Petshop, Pethaus)
- ✅ Multi-channel analytics (Online, Shop, Affiliate, B2B, Service)
- ✅ Discount and promotion tracking (online + offline)
- ✅ Time intelligence (10 period flags)
- ✅ Customer integration (unified IDs, loyalty)
- ✅ Item hierarchy (category, subcategory, brand, division)
- ✅ Location mapping (stores, cities)
- ✅ Dubai timezone (UTC+4)

---

## Comparison: Old vs. New Model

| Aspect | Old fct_commercial | New fact_commercial |
|--------|-------------------|---------------------|
| **Created** | June 23, 2025 | August 8, 2025 |
| **Status** | Deleted July 7 | Active |
| **Grain** | Order level | Transaction line level |
| **Focus** | Hyperlocal delivery | Commercial analytics |
| **Primary Use** | Delivery service adoption | Profitability & channel performance |
| **Data Source** | Orders (aggregated) | ERP value entries (transactional) |
| **Fields** | ~50 (estimated) | 90+ |
| **Key Metrics** | Order value, refund amount | Sales gross/actual, cost, discount |
| **Special Analysis** | Hyperlocal segmentation | Time period comparisons |

---

## Business Value Evolution

### **Era 1 Value (June - July 2025):**
- ✅ Measure hyperlocal launch impact
- ✅ Track 60-minute delivery adoption
- ✅ Segment customers by delivery preference
- ✅ Monitor revenue shift to express services

### **Era 2 Value (August 2025 - Present):**
- ✅ Line-level profitability analysis
- ✅ Discount effectiveness measurement
- ✅ Channel performance comparison
- ✅ Item-level margin analysis
- ✅ Promotion campaign ROI tracking
- ✅ Multi-company commercial reporting
- ✅ Period-over-period trend analysis (10 time flags)
- ✅ Customer behavior across products/channels
- ✅ Financial reconciliation with ERP

---

## Recommended Future Enhancements

### **1. Documentation & Testing**
- Add schema.yml with field descriptions and tests
- Add generic tests (not_null, unique on document_no_ + line_no)
- Add singular tests for business rules (e.g., gross >= actual)

### **2. Performance Optimization**
- Consider incremental materialization (keyed on posting_date)
- Add indexes on frequently filtered fields (posting_date, sales_channel)

### **3. Business Logic**
- Add calculated margin fields (margin = actual - cost)
- Add margin percentage ((actual - cost) / actual)
- Add YoY growth calculations using time period flags

### **4. Data Quality**
- Document why specific documents are excluded
- Add data quality monitoring (row counts, amount totals by period)
- Consider snapshot for historical tracking

### **5. Integration**
- Link to dim_customers for customer lifetime value analysis
- Link to dim_items for product performance dashboards
- Consider pre-aggregated summary tables for performance

---

**Document Created:** 2025-11-15
**Analysis Period:** June 23, 2025 - October 25, 2025 (124 days)
**Total Commits Analyzed:** 7 (2 old model + 5 new model)
**Current Model Size:** 127 lines
**Fields Tracked:** 90+ commercial metrics
