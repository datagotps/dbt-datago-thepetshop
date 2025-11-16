# Feature Name: Customer Cohort Analysis

## 1. Business Purpose
Enables **time-based customer segmentation and retention analysis** by tracking customer behavior across their lifecycle, from acquisition through repeat purchases.

**Why it was built**: To answer critical business questions about customer retention, lifetime value, and purchase patterns:
- How well do we retain customers acquired in different months/quarters?
- What percentage of customers from each cohort make repeat purchases?
- How long does it take for customers to return for their next purchase?
- Which acquisition periods produce the most valuable long-term customers?

**Business Value**:
- Track cohort retention rates over time (Month 0, Month 1, Month 2, etc.)
- Identify high-performing acquisition periods
- Measure customer lifecycle velocity (new → repeat → loyal)
- Compare performance across acquisition channels and time periods
- Calculate Customer Lifetime Value (CLV) by cohort
- Optimize marketing spend by targeting high-retention cohorts

**Dashboard Usage**: Powers customer retention dashboards, cohort analysis matrices, lifetime value reports, and acquisition performance tracking across marketing campaigns.

## 2. Technical Overview (dbt)

**Models Created/Modified**:

*Core Customer Analytics:*
- `int_customers` - Customer master with RFM segmentation + **acquisition cohort** classification
- `int_orders` - Order-level analysis with **recency cohorts** + comprehensive **monthly/quarterly/yearly cohort fields**
- `dim_customers` - Customer dimension table with cohort segments for BI reporting
- `fact_orders` - Order fact table with cohort tracking

**Upstream Sources**:
- `int_order_lines` - Line-item level sales data
- Order data aggregated at customer and order level

**Downstream Consumers**:
- Customer retention dashboards
- Cohort analysis reports
- Marketing performance analytics
- Customer lifetime value models

**Key SQL Logic**:

*Acquisition Cohort (int_customers):*
- Classifies customers based on their **first purchase date** (`customer_acquisition_date`)
- Segments: MTD, Month-Year (e.g., "Jan 25"), Year XX (e.g., "Year 24"), "Year 19 & Before"
- Includes `acquisition_cohort_rank` for sorting (100-1000 range)
- Logic uses CURRENT_DATE() for dynamic monthly/yearly grouping

*Recency Cohort (int_orders):*
- Classifies **each order** based on time since customer's previous order
- Segments: New Customer, Recent Return (<1M), Month 1-6 Return, Dormant Return (>6M)
- Tracks customer engagement status: New, Active, Recent, At Risk, Reactivated (Churn/Inactive/Lost)
- Uses `DATE_DIFF` between current order and previous order date

*Monthly Cohort Tracking (int_orders):*
- `cohort_month_label` - "Month 0", "Month 1", "Month 2" (relative to acquisition)
- `cohort_month_number` - Numeric month since acquisition (0, 1, 2, ...)
- `cohort_month_actual_name` - "Jan 2025", "Feb 2025" (actual calendar month)
- `cohort_month_actual_sort` - 202501, 202502 (sortable integer)

*Quarterly Cohort Tracking (int_orders):*
- `cohort_quarter_label` - "Q 0", "Q 1", "Q 2" (relative quarters since acquisition)
- `cohort_quarter_number` - Numeric quarter since acquisition
- `cohort_quarter_actual_name` - "Q1 2025", "Q2 2025" (actual calendar quarter)
- `cohort_quarter_actual_sort` - 20251, 20252 (sortable)

*Yearly Cohort Tracking (int_orders):*
- `cohort_year_label` - "Year 0", "Year 1", "Year 2" (years since acquisition)
- `cohort_year_number` - Numeric years since acquisition
- `cohort_year_actual_name` - "2025", "2026" (actual calendar year)
- `cohort_year_actual_sort` - 2025, 2026 (sortable)

*Additional Cohort Dimensions (int_orders):*
- `acquisition_quarter` - "Q3 2025" (quarter customer was acquired)
- `acquisition_month` - "Aug 2025" (month customer was acquired)
- `acquisition_year` - 2025 (year customer was acquired)
- `acquisition_week` - "Week 32 2025"
- `weeks_since_acquisition` - Numeric weeks since first purchase
- `is_acquisition_month` - TRUE/FALSE flag for acquisition month
- `cohort_age_bucket` - Day 0, Month 1, Month 2, Month 3, Month 4-6, Month 7-12, Month 13+

*Customer Sequencing:*
- `customer_order_sequence` - Order number for each customer (1, 2, 3, ...)
- `total_lifetime_orders` - Running total of customer's orders
- `transaction_frequency_segment` - 1st Purchase, 2nd Purchase, ..., Repeat Buyer (8+ Orders)
- `new_vs_returning` - Simple New/Returning classification

**KPI Definitions Created**:
- `acquisition_cohort` - Time period when customer first purchased
- `recency_cohort` - Time since last purchase for each order
- `cohort_month_number` / `cohort_quarter_number` / `cohort_year_number` - Relative time since acquisition
- `customer_order_sequence` - Purchase ordinal number per customer
- `days_since_last_order` - Recency metric for retention analysis
- `customer_engagement_status` - New/Active/Recent/At Risk/Reactivated lifecycle stage

## 3. Model Lineage (high-level)

```
int_order_lines (line-item sales)
        │
        ▼
   int_orders (order aggregation)
        │
        ├─► Customer Acquisition CTE ──────► customer_acquisition_date
        │                                     acquisition_month/quarter/year
        │
        ├─► Order Sequence CTE ────────────► customer_order_sequence
        │                                     previous_order_date
        │
        ├─► Retention Metrics CTE ─────────► days_since_last_order
        │                                     recency_cohort
        │                                     customer_engagement_status
        │
        └─► Final Enhanced CTE ────────────► cohort_month_label/number
                                              cohort_quarter_label/number
                                              cohort_year_label/number
                                              weeks_since_acquisition
                                              is_acquisition_month
                                              cohort_age_bucket
        │
        ▼
   fact_orders (BI layer - order-level cohort analysis)
        │
        ▼
   int_customers (customer aggregation)
        │
        ├─► Customer Base Metrics CTE ─────► customer_acquisition_date
        │                                     first_order_date
        │                                     total_order_count
        │
        ├─► Customer Calculated Metrics ──► months_since_acquisition
        │                                     customer_tenure_days
        │
        └─► Customer Segments CTE ─────────► acquisition_cohort
                                              acquisition_cohort_rank
        │
        ▼
   dim_customers (BI layer - customer-level cohorts)
```

## 4. Important Fields Added

### Acquisition Cohort Fields (int_customers)

| Field | Description |
|-------|-------------|
| `customer_acquisition_date` | Date of customer's first order (cohort assignment basis) |
| `acquisition_cohort` | Time period label: "MTD", "Jan 25", "Year 24", "Year 23", "Year 19 & Before" |
| `acquisition_cohort_rank` | Sort order for cohorts: 1000 (MTD), 900+ (current year months), 800 (last year), 100 (6+ years ago) |
| `months_since_acquisition` | Number of complete months since first purchase |
| `customer_tenure_days` | Total days since acquisition |
| `customer_tenure_segment` | 1 Month, 3 Months, 6 Months, 1 Year, 2 Years, 3 Years, 4+ Years |

### Recency Cohort Fields (int_orders)

| Field | Description |
|-------|-------------|
| `recency_cohort` | New Customer, Recent Return (<1M), Month 1-6 Return, Dormant Return (>6M) |
| `days_since_last_order` | Days between this order and customer's previous order |
| `customer_engagement_status` | New, Active (≤30d), Recent (≤60d), At Risk (≤90d), Reactivated (Churn/Inactive/Lost) |
| `new_vs_returning` | Simple binary: New or Returning customer |
| `customer_order_sequence` | Sequential order number per customer (1st, 2nd, 3rd, ...) |
| `transaction_frequency_segment` | 1st Purchase, 2nd Purchase, ... 7th Purchase, Repeat Buyer (8+ Orders) |

### Monthly Cohort Fields (int_orders)

| Field | Description |
|-------|-------------|
| `cohort_month_label` | Relative month label: "Month 0", "Month 1", "Month 2", ... |
| `cohort_month_number` | Numeric month since acquisition: 0, 1, 2, 3, ... |
| `cohort_month_actual_name` | Actual calendar month: "Jan 2025", "Feb 2025", ... |
| `cohort_month_actual_sort` | Sortable integer: 202501, 202502, ... |

### Quarterly Cohort Fields (int_orders)

| Field | Description |
|-------|-------------|
| `cohort_quarter_label` | Relative quarter label: "Q 0", "Q 1", "Q 2", ... |
| `cohort_quarter_number` | Numeric quarter since acquisition: 0, 1, 2, ... |
| `cohort_quarter_actual_name` | Actual calendar quarter: "Q1 2025", "Q2 2025", ... |
| `cohort_quarter_actual_sort` | Sortable integer: 20251, 20252, ... |

### Yearly Cohort Fields (int_orders)

| Field | Description |
|-------|-------------|
| `cohort_year_label` | Relative year label: "Year 0", "Year 1", "Year 2", ... |
| `cohort_year_number` | Numeric years since acquisition: 0, 1, 2, ... |
| `cohort_year_actual_name` | Actual calendar year: "2025", "2026", ... |
| `cohort_year_actual_sort` | Sortable integer: 2025, 2026, ... |

### Acquisition Dimension Fields (int_orders)

| Field | Description |
|-------|-------------|
| `acquisition_quarter` | Quarter customer was acquired: "Q3 2025" |
| `acquisition_month` | Month customer was acquired: "Aug 2025" |
| `acquisition_year` | Year customer was acquired: 2025 |
| `acquisition_week` | Week customer was acquired: "Week 32 2025" |
| `weeks_since_acquisition` | Total weeks since first purchase |
| `is_acquisition_month` | TRUE if order is in same month as acquisition |
| `cohort_age_bucket` | Day 0, Month 1-3, Month 4-6, Month 7-12, Month 13+ |
| `acquisition_month_sort` | Sortable acquisition month: 202508 |
| `acquisition_quarter_sort` | Sortable acquisition quarter: 20253 |

## 5. Git Commit History Summary

| Commit ID | Author | Date | Summary |
|-----------|--------|------|---------|
| `3b3d38b` | Anmar Abbas DataGo | 2025-08-08 | **Initial cohort implementation**: Added acquisition_cohort to int_customers, recency_cohort and full monthly/quarterly/yearly cohort tracking to int_orders |
| `fcc4e4c` | Anmar Abbas DataGo | 2025-08-14 | **Enhanced customer analytics**: Expanded dim_customers with 661 line changes, added detailed customer model analysis documentation |
| `6f818cd` | Anmar Abbas DataGo | 2025-10-06 | Refinements to order and customer models |
| `6cd3a1f` | Anmar Abbas DataGo | 2025-10-15 | Latest updates to customer and order analytics |

**Key Changes in Commits**:
- **3b3d38b**: Core cohort analysis framework - acquisition cohort (int_customers), recency cohort + monthly/quarterly/yearly cohort fields (int_orders)
- **fcc4e4c**: Major enhancement to dim_customers with additional customer segmentation and analytics capabilities
- Subsequent commits refined the cohort logic and customer analysis models

## 6. Limitations / Assumptions

**Assumptions**:
- Customer acquisition date = first order date in the system (may not capture pre-system purchases)
- `unified_customer_id` correctly identifies unique customers across channels (dependent on customer deduplication logic)
- CURRENT_DATE() used for dynamic cohort assignment (cohorts will shift as time progresses)
- Orders are properly sequenced chronologically by `order_date`
- All orders have valid `unified_customer_id` (NULL values excluded from cohort analysis)

**Limitations**:
- Historical changes in customer acquisition date are not tracked (no slowly changing dimension)
- Cohort labels are relative to CURRENT_DATE() - not fixed point-in-time snapshots
- No handling for merged/split customer records (relies on upstream customer deduplication)
- Yearly cohorts use calendar years (not fiscal years or rolling 12-month periods)
- Recency cohort thresholds are fixed (30d, 60d, 90d, etc.) - not dynamically calculated per customer segment
- No separate cohorts for different acquisition channels (online vs offline)
- Test customers (200+ orders) flagged but not excluded from cohort calculations
- Cohort analysis doesn't account for refunds/returns in retention calculation

**Future Improvements**:
- Add cohort-specific KPIs: retention rate, repeat purchase rate, CLV by cohort
- Implement channel-specific acquisition cohorts (Online Q1 2025, Shop Q2 2025)
- Create cohort performance metrics (cohort size, avg order value, repurchase rate by month)
- Add predictive cohort fields (expected next purchase date, churn probability)
- Build cohort retention matrix (M0, M1, M2, ... retention percentages)
- Implement fiscal year cohort option for financial reporting
- Add cohort comparison metrics (cohort A vs cohort B performance)
- Create cohort lifecycle segments (New → Active → At Risk → Churned progression tracking)
- Add generic dbt tests for cohort field completeness and data quality
- Implement incremental cohort calculations for performance optimization
