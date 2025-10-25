SELECT

-- Order Identifiers
source_no_,                        -- dim (customer ID list)
unified_customer_id,               -- dim (key)
unified_order_id,                  -- dim (key)
document_no_,                      -- dim (ERP doc number)
web_order_id,                      -- dim (web order ID)
loyality_member_id,                -- dim (member ID or null)

-- Order Metrics
order_date,                        -- dim (date)
order_value,                       -- fact (AED amount for sales)
refund_amount,                     -- fact (AED negative amount)
total_order_amount,                -- fact (AED net amount)
line_items_count,                  -- fact (count of lines)
document_no_count,                 -- fact (count of docs)
positive_line_items,               -- fact (count positive lines)
document_type,                     -- dim: Sales Invoice, Sales Credit Memo, etc
transaction_type,                  -- dim: Sale, Refund, Other

-- Channel & Location
sales_channel,                     -- dim: Online, Shop, Affiliate, B2B, Service
store_location,                    -- dim: Online, DIP, FZN, REM, UMSQ, WSL, CREEK, DSO, MRI, RAK
store_location_sort,               -- dim: 1-99 (sort)
platform,                          -- dim: website, Android, iOS, CRM
order_type,                        -- dim: EXPRESS, NORMAL, EXCHANGE
paymentgateway,                    -- dim: Cash, Card, Tabby, COD, etc
paymentmethodcode,                 -- dim: PREPAID, COD

-- Customer Info
customer_name,                     -- dim (text)
std_phone_no_,                     -- dim (standardized phone)
raw_phone_no_,                     -- dim (original phone)
duplicate_flag,                    -- dim: Yes, No
customer_identity_status,          -- dim: Verified, Unverified

-- Time Dimensions
order_month,                       -- dim (2025-01-01 format)
order_week,                        -- dim (week date)
order_year,                        -- fact (2025)
order_month_num,                   -- fact (1-12)
year_month,                        -- dim: 2025-01

-- Hyperlocal Analysis
hyperlocal_period,                 -- dim: Pre-Launch, Post-Launch
delivery_service_type,             -- dim: 60-Min Hyperlocal, 4-Hour Express, Standard, Exchange, Refund
service_tier,                      -- dim: Express Service, Standard Service, Refund Transaction
hyperlocal_order_flag,             -- dim: Hyperlocal Order, Non-Hyperlocal Order, Refund
days_since_hyperlocal_launch,      -- fact (days from Jan 16)

-- Customer Segmentation
hyperlocal_customer_segment,       -- dim: Acquired Pre-Launch, Post-Launch
hyperlocal_usage_flag,             -- dim: Used Hyperlocal, Never Used
hyperlocal_customer_detailed_segment, -- dim: Post-HL Acq + HL User, Pre-HL Acq + HL User, etc
hyperlocal_customer_detailed_segment_order, -- dim: 1-5 (sort)

-- Order Classifications
order_channel,                     -- dim: Online, Store, Unknown
order_channel_detail,              -- dim: specific store/platform
payment_category,                  -- dim: Cash/COD, Card Payment, BNPL, Loyalty/Points
order_size_category,               -- dim: Large (500+), Medium (200-499), Small (100-199), Micro (<100)
customer_tenure_days_at_order,     -- fact (days since first order)
customer_lifecycle_at_order,       -- dim: New Customer Order, Returning Customer Order, Refund

-- Retention Metrics
customer_order_sequence,           -- fact (1st, 2nd, 3rd order...)
channel_order_sequence,            -- fact (order # in channel)
previous_order_date,               -- dim (date)
previous_channel_order_date,       -- dim (date)
total_lifetime_orders,             -- fact (total count)
total_channel_orders,              -- fact (channel count)
days_since_last_order,             -- fact (days)
days_since_last_channel_order,     -- fact (days)
recency_cohort,                    -- dim: New Customer, Recent Return, Month 1-6 Return, Dormant
customer_engagement_status,        -- dim: New, Active, Recent, At Risk, Reactivated
customer_engagement_status_sort,   -- dim: 1-8 (sort)
new_vs_returning,                  -- dim: New, Returning
is_test_customer,                  -- dim: TRUE, FALSE (200+ orders)

-- Transaction Frequency
transaction_frequency_segment,     -- dim: 1st Purchase, 2nd Purchase... 8+ Orders
transaction_frequency_segment_sort, -- dim: 1-9 (sort)

-- Acquisition Info
acquisition_channel,               -- dim: Online, Shop, Other
acquisition_store,                 -- dim: DIP, FZN, REM, etc
acquisition_platform,              -- dim: website, Android, iOS
acquisition_payment_method,        -- dim: Cash, Card, Tabby, etc
channels_used_count,               -- fact (count)
stores_visited_count,              -- fact (count)
channel_preference_type,           -- dim: Hybrid, Online, Shop

-- Cohort Analysis
acquisition_quarter,               -- dim: Q1 2025, Q4 2024, etc
acquisition_month,                 -- dim: Jan 2025, Dec 2024, etc
acquisition_year,                  -- fact (2024, 2025)
acquisition_quarter_sort,          -- fact (202501)
acquisition_month_sort,            -- fact (202501)
weeks_since_acquisition,           -- fact (weeks count)
acquisition_week,                  -- dim: Week 03 2025
is_acquisition_month,              -- dim: TRUE, FALSE
cohort_age_bucket,                 -- dim: Day 0, Month 1-3, Month 4-6, Month 7-12, Month 13+
acquisition_month_date,            -- dim (date)
customer_acquisition_date,         -- dim (date)
order_value_bucket,                -- dim: 0-50, 50-100, 100-200, 200-500, 500-1000, 1000+
order_value_bucket_sort,           -- dim: 1-6 (sort)

-- Cohort Time Periods
cohort_year_actual_name,           -- dim: 2024, 2025, etc
cohort_quarter_actual_name,        -- dim: Q1 2025, Q2 2025, etc
cohort_month_actual_name,          -- dim: Jan 2025, Feb 2025, etc
cohort_year_label,                 -- dim: Year 0, Year 1, Year 2
cohort_quarter_label,              -- dim: Q 0, Q 1, Q 2, etc
cohort_month_label,                -- dim: Month 0, Month 1, Month 2, etc
cohort_year_actual_sort,           -- fact (2024, 2025)
cohort_quarter_actual_sort,        -- fact (202501, 202502)
cohort_month_actual_sort,          -- fact (202501, 202502)
cohort_year_number,                -- fact (0, 1, 2...)
cohort_quarter_number,             -- fact (0, 1, 2, 3...)
cohort_month_number,               -- fact (0, 1, 2, 3...)

-- REPORT METADATA
DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 4 HOUR) AS report_last_updated_at, 



FROM {{ ref('int_orders') }}

where sales_channel in ('Online','Shop') 


and (order_date BETWEEN '2025-01-01' AND '2025-09-30'
       OR order_date BETWEEN '2024-12-01' AND '2024-12-31'
       OR order_date BETWEEN '2024-01-01' AND '2024-01-31'
      )



/*
AND (
    (order_date >= '2025-01-01' AND order_date < '2025-02-01')  -- Jan 2025
    OR (order_date >= '2024-01-01' AND order_date < '2024-02-01')  -- Jan 2024
    OR (order_date >= '2024-12-01' AND order_date < '2025-01-01')  -- Dec 2024
)
*/