select


-- Customer Identifiers & Basic Info
unified_customer_id,              -- dim (key)
source_no_,                        -- dim (customer ID)
customer_name,                     -- dim (text)
std_phone_no_,                     -- dim (standardized phone)
customer_identity_status,          -- dim: Verified, Unverified
duplicate_flag,                    -- dim: Yes, No
raw_phone_no_,                     -- dim (original phone)
loyality_member_id,                -- dim (member ID or null)
active_months_count,               -- fact (count)
loyalty_enrollment_status,         -- dim: Enrolled, Not Enrolled

-- Customer Dates
customer_acquisition_date,         -- dim (date)
first_order_date,                  -- dim (date)
last_order_date,                   -- dim (date)

-- Channel Usage
stores_used,                       -- dim: DIP | FZN | REM | etc
platforms_used,                    -- dim: website | Android | iOS

-- Order Counts
total_order_count,                 -- fact (count)
online_order_count,                -- fact (count)
offline_order_count,               -- fact (count)

-- Revenue Metrics
total_sales_value,                 -- fact (AED amount)
online_sales_value,                -- fact (AED amount)
offline_sales_value,               -- fact (AED amount)
ytd_sales,                         -- fact (AED amount)
mtd_sales,                         -- fact (AED amount)

-- Hyperlocal Metrics
pre_hyperlocal_orders,             -- fact (count before Jan 16)
pre_hyperlocal_revenue,            -- fact (AED before Jan 16)
post_hyperlocal_orders,            -- fact (count after Jan 16)
post_hyperlocal_revenue,           -- fact (AED after Jan 16)
hyperlocal_60min_orders,           -- fact (60-min delivery count)
hyperlocal_60min_revenue,          -- fact (60-min AED)
express_4hour_orders,              -- fact (4-hour delivery count)
express_4hour_revenue,             -- fact (4-hour AED)

-- Order Lists
document_ids_list,                 -- dim: order1 | order2 | ...
online_order_ids,                  -- dim: web orders list
offline_order_ids,                 -- dim: shop orders list

-- Acquisition Details
first_acquisition_store,           -- dim: DIP, FZN, REM, etc
first_acquisition_platform,        -- dim: website, Android, iOS
customer_acquisition_channel,      -- dim: Online, Shop, Other
first_acquisition_paymentgateway,  -- dim: Cash, Card, Tabby, etc
first_acquisition_order_type,      -- dim: EXPRESS, NORMAL, EXCHANGE
customer_acquisition_channel_detail, -- dim: store/platform name

-- RFM Core Metrics
recency_days,                      -- fact (days since last order)
customer_tenure_days,              -- fact (days since first order)
frequency_orders,                  -- fact (total orders)
monetary_total_value,              -- fact (total spend AED)
monetary_avg_order_value,          -- fact (avg order AED)
avg_monthly_demand,                -- fact (avg monthly AED)
months_since_acquisition,          -- fact (months count)

-- Segmentation
hyperlocal_customer_segment,       -- dim: Acquired Pre-Launch, Post-Launch
m1_retention_segment,              -- dim: M1 Retention Target, Not M1 Target
transacted_last_month,             -- dim: Yes, No
transacted_current_month,          -- dim: Yes, No
hyperlocal_usage_flag,             -- dim: Used Hyperlocal, Never Used
delivery_service_preference,       -- dim: Both Express Types, 60-Min Only, 4-Hour Only, Standard Only
hyperlocal_customer_detailed_segment, -- dim: Post-HL Acq + HL User, Pre-HL Acq + HL User, etc
hyperlocal_customer_detailed_segment_order, -- dim: 1-5 (sort)
purchase_frequency_bucket,         -- dim: 1 Order, 2-3, 4-6, 7-10, 11+
purchase_frequency_bucket_order,   -- dim: 1-6 (sort)
customer_recency_segment,          -- dim: Active, Recent, At Risk, Churn, Inactive, Lost
customer_recency_segment_order,    -- dim: 1-6 (sort)
customer_tenure_segment,           -- dim: 1 Month, 3 Months, 6 Months, 1 Year, 2 Years, 3 Years, 4+ Years
customer_tenure_segment_order,     -- dim: 1-8 (sort)
customer_type,                     -- dim: New, Repeat, One-Time
customer_channel_distribution,     -- dim: Hybrid, Online, Shop
acquisition_cohort,                -- dim: MTD, Jan 25, Year 24, Year 23, etc
acquisition_cohort_rank,           -- dim: 100-1000 (sort)

-- Multiple Source Tracking
all_source_nos,                    -- dim: source1 | source2 | ...
duplicate_customer_count,          -- fact (count of sources)

-- Order Patterns
avg_days_between_orders,           -- fact (days avg)
stddev_days_between_orders,        -- fact (days std dev)

-- RFM Segments
rfm_segment,                       -- dim: 555, 444, 333, etc (3 digits)
customer_value_segment,            -- dim: Top 1%, Top 20%, Middle 30-60%, Bottom 40%
customer_value_segment_order,      -- dim: 1-4 (sort)
customer_rfm_segment,              -- dim: Champions, Loyal, Cant Lose, At Risk, Lost, etc
customer_rfm_segment_order,        -- dim: 1-7 (sort)
r_score,                           -- fact: 1-5 (recency score)
f_score,                           -- fact: 1-5 (frequency score)
m_score,                           -- fact: 1-5 (monetary score)

-- Purchase Behavior
purchase_frequency_type,           -- dim: New, One-Time, Weekly, Monthly, Quarterly, Annual, Inconsistent
purchase_frequency_type_order,     -- dim: 1-7 (sort)
customer_pattern_type,             -- dim: No Pattern, Perfect, Highly Consistent, Variable, etc
customer_pattern_type_order,       -- dim: 0-6 (sort)

-- Churn Analytics
churn_risk_score,                  -- fact: 0-100 (percentage)
churn_risk_level,                  -- dim: New Customer, Low, Medium, High, Critical
churn_risk_level_order,            -- dim: 0-4 (sort)
days_until_expected_order,         -- fact (days predicted)
is_overdue,                        -- dim: Yes, No
overdue_confidence,                -- dim: On Schedule, 68% Confidence, 95% Confidence, 99.7% Confidence
churn_action_required,             -- dim: Single Order-Monitor, Dormant, Severely Overdue, At Risk, On Track

-- System Metadata

DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 4 HOUR) AS report_last_updated_at,


FROM {{ ref('int_customers') }} as a

where customer_acquisition_channel in ('Online','Shop') 

AND (
    (customer_acquisition_date >= '2025-01-01' AND customer_acquisition_date < '2025-02-01')  -- Jan 2025
    OR (customer_acquisition_date >= '2024-01-01' AND customer_acquisition_date < '2024-02-01')  -- Jan 2024
    OR (customer_acquisition_date >= '2024-12-01' AND customer_acquisition_date < '2025-01-01')  -- Dec 2024
)