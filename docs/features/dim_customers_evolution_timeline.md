# dim_customers Model Evolution Timeline

**Model:** `models/3_fct/dim_customers.sql`
**Total Commits:** 6
**Development Period:** July 7, 2025 - October 15, 2025 (100 days)
**Primary Developer:** Anmar Abbas DataGo

---

## Overview

The `dim_customers` model has undergone significant architectural changes over its 100-day development lifecycle. The model evolved from a simple customer dimension to a sophisticated customer analytics layer with RFM scoring, churn prediction, and customer segmentation - with two major architectural rewrites along the way.

---

## Commit Timeline & Feature Evolution

### 📅 **Commit 1: Initial Creation**
**Date:** July 7, 2025
**Commit ID:** `0db360a8270b5669ce0f5f11eee145e5e73d6449`
**Lines Added:** 127
**Feature Name:** **Customer Dimension Foundation**

**What Was Built:**
- **Initial customer dimension model** with basic customer analytics
- **Data Source:** Directly references `int_customers` model
- **Primary Key:** `source_no_` (ERP customer ID)

**Features Added:**
1. **Customer Identification**
   - source_no_ (ERP customer ID)
   - name (customer full name)
   - std_phone_no_ (standardized phone)
   - customer_identity_status
   - loyality_member_id

2. **Core Activity Metrics**
   - active_months_count (distinct months with purchases)
   - loyalty_enrollment_status (Enrolled/Not Enrolled)

3. **Date Tracking**
   - customer_acquisition_date
   - first_order_date
   - last_order_date

4. **Channel Analytics**
   - stores_used (pipe-delimited list)
   - platforms_used (pipe-delimited list)
   - total_order_count, online_order_count, offline_order_count

5. **Revenue Metrics**
   - total_sales_value, online_sales_value, offline_sales_value
   - ytd_sales, mtd_sales
   - monetary_avg_order_value, avg_monthly_demand

6. **Hyperlocal Launch Analytics** (Jan 16, 2025 launch)
   - pre_hyperlocal_orders/revenue
   - post_hyperlocal_orders/revenue
   - hyperlocal_60min_orders/revenue
   - express_4hour_orders/revenue
   - hyperlocal_customer_segment (Pre-Launch/Post-Launch acquisition)
   - hyperlocal_usage_flag
   - delivery_service_preference

7. **Customer Segmentation**
   - purchase_frequency_bucket (1 Order, 2-3, 4-6, 7-10, 11+)
   - customer_recency_segment (Active, Recent, At Risk, Churn, Inactive, Lost)
   - customer_tenure_segment (1 Month, 3 Months, 6 Months, 1 Year, 2+ Years)
   - customer_type (New, Repeat, One-Time)
   - customer_channel_distribution (Hybrid, Online, Shop)

8. **RFM Analysis** (Initial Implementation)
   - r_score, f_score, m_score (1-5 scores)
   - rfm_segment (3-digit code like "555")
   - customer_value_segment (Top 1%, Top 20%, Middle, Bottom)
   - customer_rfm_segment (Champions, Loyal, Cant Lose Them, Potential Loyalists, New, At Risk, Lost)

9. **Churn Analytics**
   - churn_risk_score (0-100)
   - churn_risk_level (New Customer, Low, Medium, High, Critical)
   - days_until_expected_order
   - is_overdue (Yes/No)
   - overdue_confidence
   - churn_action_required

10. **Order Pattern Analysis**
    - avg_days_between_orders
    - stddev_days_between_orders
    - customer_pattern_type (Perfect Consistency, Variable, etc.)
    - purchase_frequency_type (Weekly, Monthly, Quarterly, Annual, etc.)

11. **Acquisition Analytics**
    - first_acquisition_store
    - first_acquisition_platform
    - customer_acquisition_channel
    - acquisition_cohort (MTD, month-year cohorts)

---

### 📅 **Commit 2: M1 Retention Tracking**
**Date:** July 7, 2025 (same day)
**Commit ID:** `2d85313925fa1524e7adef1dd3f386b1d7e3a6e1`
**Lines Added:** 5
**Feature Name:** **Month-1 Retention Metrics**

**What Was Added:**
1. **m1_retention_segment** - Identifies customers eligible for M1 retention campaigns
2. **transacted_last_month** - Flag if customer purchased in previous month
3. **transacted_current_month** - Flag if customer purchased in current month

**Business Purpose:**
Enable tracking of monthly retention cohorts and identify customers at risk of not returning in their second month (M1 churn prevention).

---

### 📅 **Commit 3: Timezone Standardization**
**Date:** July 18, 2025
**Commit ID:** `f95f266cd2b8f39be61459eab55e2d3b06dff2a3`
**Lines Added:** 3, Lines Changed:** 1
**Feature Name:** **Dubai Time (UTC+4) Reporting**

**What Was Changed:**
- **Before:** `CURRENT_DATETIME() AS report_last_updated_at`
- **After:** `DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 4 HOUR) AS report_last_updated_at`

**Business Purpose:**
Align all report timestamps with Dubai local time (UTC+4) for consistency across business reporting.

---

### 📅 **Commit 4: Phone-Based Aggregation (Major Refactor #1)**
**Date:** August 14, 2025
**Commit ID:** `fcc4e4cb274c128ec5f70e586a8a3e7294c94258`
**Lines Added:** 543, Lines Removed: 126
**Net Change:** +417 lines
**Feature Name:** **Customer Deduplication via Phone Number**

**Major Architectural Change:**
Completely rewrote the model to **group customers by phone number** instead of source_no_ to handle duplicate customer IDs.

**What Was Built:**
1. **Phone Aggregation CTE**
   - Groups all customer records by `std_phone_no_`
   - Aggregates multiple source_no_ into single customer view
   - Uses dynamic grouping: phone if valid, otherwise source_no_

2. **New Duplicate Tracking Fields**
   - duplicate_customer_count (count of source_no_ per phone)
   - all_source_nos (pipe-delimited list of all customer IDs)
   - all_loyalty_ids (pipe-delimited list of all loyalty IDs)
   - has_duplicate_customer_ids (Yes/No flag)

3. **RFM Percentile Calculation**
   - Added `rfm_percentiles` CTE for dynamic percentile-based scoring
   - Calculates 20th, 40th, 60th, 80th percentiles for R, F, M scores

4. **Aggregation Logic**
   - SUM for all order counts and revenue metrics
   - MAX for status fields and dates
   - STRING_AGG for lists (stores, platforms, order IDs)
   - ARRAY_AGG with LIMIT 1 for acquisition details (earliest record wins)

**Business Purpose:**
Solve the duplicate customer problem where single customers had multiple source_no_ values in ERP system, causing inflated customer counts and fragmented purchase history.

---

### 📅 **Commit 5: Unified Customer ID (Major Refactor #2)**
**Date:** October 6, 2025
**Commit ID:** `6f818cd1b3e148fd8cb6995a493932f46e15956d`
**Lines Added:** 576, Lines Removed: 1,379
**Net Change:** -803 lines
**Feature Name:** **Simplified Architecture with Unified Customer ID**

**Major Architectural Change:**
**Rolled back phone aggregation** and moved all deduplication logic upstream to `int_customers` model. Switched to using `unified_customer_id` as the primary key.

**What Changed:**
1. **Removed Phone Aggregation**
   - Deleted entire phone_aggregated CTE (removed complex aggregation logic)
   - Deleted rfm_percentiles CTE (moved to int_customers)
   - Simplified to direct SELECT from int_customers

2. **New Primary Key**
   - **Before:** Grouped by `std_phone_no_`
   - **After:** Uses `unified_customer_id` from int_customers (one record per customer)

3. **Added New Fields from int_customers**
   - unified_customer_id (new primary key)
   - raw_phone_no_ (original unprocessed phone)
   - all_source_nos (tracking multiple source_no_ per customer)
   - duplicate_customer_count (moved from this model to int_customers)

4. **Simplified Logic**
   - All RFM calculations now happen in int_customers
   - All aggregations now happen in int_customers
   - dim_customers becomes a simple SELECT with field comments

5. **Added WHERE Filter**
   - `where customer_acquisition_channel in ('Online','Shop')`
   - Excludes non-standard acquisition channels

**Business Purpose:**
- **Performance Improvement**: Eliminated redundant aggregation (moved to int_customers)
- **Maintainability**: Single source of truth for RFM calculations
- **Clarity**: dim_customers now just a documented view of int_customers

---

### 📅 **Commit 6: Filter Adjustment**
**Date:** October 15, 2025
**Commit ID:** `6cd3a1f3fa88a90bce7000a0a9136448e9a3607c`
**Lines Added:** 27 (int_customers), Lines Changed: 4 (dim_customers)
**Feature Name:** **Date Filter Comment-Out for Testing**

**What Was Changed in dim_customers:**
- Commented out the date acquisition filter that limited results to:
  - January 2025
  - December 2024
  - January 2024

**Before:**
```sql
AND (
    (customer_acquisition_date >= '2025-01-01' AND customer_acquisition_date < '2025-02-01')
    OR (customer_acquisition_date >= '2024-01-01' AND customer_acquisition_date < '2024-02-01')
    OR (customer_acquisition_date >= '2024-12-01' AND customer_acquisition_date < '2025-01-01')
)
```

**After:**
```sql
/*
AND (
    (customer_acquisition_date >= '2025-01-01' AND customer_acquisition_date < '2025-02-01')
    OR (customer_acquisition_date >= '2024-01-01' AND customer_acquisition_date < '2024-02-01')
    OR (customer_acquisition_date >= '2024-12-01' AND customer_acquisition_date < '2025-01-01')
)
*/
```

**Business Purpose:**
Enable full historical customer analysis (not limited to specific months). Likely for comprehensive reporting or testing purposes.

---

## Summary of Features by Category

### 🎯 **Customer Identification & Demographics**
- **Initial (July 7):** source_no_, name, std_phone_no_, customer_identity_status, loyality_member_id
- **Aug 14:** Added duplicate tracking (all_source_nos, duplicate_customer_count, has_duplicate_customer_ids)
- **Oct 6:** Added unified_customer_id, raw_phone_no_

### 📊 **RFM & Segmentation**
- **Initial (July 7):** Full RFM scoring (r_score, f_score, m_score), 7 RFM segments, customer value tiers
- **Aug 14:** Added dynamic percentile-based RFM calculation
- **Oct 6:** Moved RFM calculation logic to int_customers

### 🔄 **Churn & Retention**
- **Initial (July 7):** Churn risk scoring, expected order dates, overdue tracking
- **July 7 (2nd commit):** Added M1 retention tracking (transacted_last_month, transacted_current_month)

### 🚚 **Hyperlocal Delivery Analytics**
- **Initial (July 7):** Complete hyperlocal delivery tracking (Jan 16, 2025 launch)
  - Pre/post launch orders and revenue
  - 60-minute delivery metrics
  - Express 4-hour delivery metrics
  - Customer segments based on hyperlocal usage

### 📈 **Behavioral Segmentation**
- **Initial (July 7):**
  - Purchase frequency buckets
  - Recency segments (Active → Lost)
  - Tenure segments (1 Month → 4+ Years)
  - Customer type (New, Repeat, One-Time)
  - Order pattern consistency

### 🕒 **Reporting & Metadata**
- **July 18:** Changed to Dubai time (UTC+4)

---

## Architecture Evolution

### **Phase 1: Foundation (July 7)**
```
int_customers → dim_customers (simple SELECT with comments)
Primary Key: source_no_
```

### **Phase 2: Phone Aggregation (Aug 14 - Oct 5)**
```
int_customers → dim_customers (phone-based aggregation with CTEs)
Primary Key: std_phone_no_
- Added phone_aggregated CTE
- Added rfm_percentiles CTE
- Complex aggregation logic in dim_customers
```

### **Phase 3: Unified Architecture (Oct 6 - Present)**
```
int_customers (all aggregation + RFM logic) → dim_customers (simple documented view)
Primary Key: unified_customer_id
- All complexity moved to int_customers
- dim_customers is thin presentation layer
- Single source of truth for customer metrics
```

---

## Current State (as of Oct 15, 2025)

**Lines of Code:** ~135 lines
**Complexity:** Low (simple SELECT statement)
**Primary Key:** unified_customer_id
**Total Fields:** 100+ customer attributes
**Materialization:** Not specified (likely view or table)

**Key Characteristics:**
- ✅ Simple, maintainable presentation layer
- ✅ All business logic in int_customers (separation of concerns)
- ✅ Comprehensive inline documentation (every field commented)
- ✅ Filters to Online/Shop channels only
- ✅ Dubai timezone (UTC+4) for reporting

---

## Lessons Learned from Evolution

### **1. Architecture Decisions**
- **Initial Approach:** Simple pass-through from int_customers
- **Middle Phase:** Tried to solve deduplication in dim layer (phone aggregation)
- **Final Approach:** Moved deduplication upstream (unified_customer_id in int_customers)
- **Lesson:** Business logic belongs in intermediate layer, not dimension layer

### **2. Performance Optimization**
- Removed redundant aggregations by moving logic to int_customers
- Reduced lines of code by 85% (from ~550 to ~135)
- Eliminated complex CTEs in favor of pre-calculated fields

### **3. Maintainability**
- Excellent inline documentation (every field has a comment)
- Clear separation: int_customers = calculation engine, dim_customers = documented interface
- Easier to debug and modify (single source of truth)

---

## Recommended Future Enhancements

1. **Add dbt Schema Documentation**
   - Create schema.yml with descriptions for all 100+ fields
   - Add generic tests (not_null, unique, relationships)

2. **Incremental Materialization**
   - Consider incremental strategy for performance
   - Add updated_at timestamp for incremental logic

3. **Historical Tracking**
   - Implement SCD Type 2 for tracking RFM segment changes over time
   - Create snapshot for monitoring customer journey

4. **Data Quality**
   - Add singular tests for RFM score ranges (1-5)
   - Add tests for segment membership consistency

5. **Business Logic Documentation**
   - Document RFM segment definitions in YAML
   - Create data dictionary for business users

---

**Document Created:** 2025-11-15
**Analysis Period:** July 7, 2025 - October 15, 2025 (100 days)
**Total Commits Analyzed:** 6
**Net Code Change:** +8 lines (127 initial → 135 final)
