SELECT

-- ORDER IDENTIFIERS (4 columns)
source_no_,
unified_order_id,
document_no_,
web_order_id,

-- ORDER CORE DATA - AGGREGATED METRICS (7 columns)
order_date,
order_value,
refund_amount,
total_order_amount,
line_items_count,
positive_line_items,
document_type_2,
transaction_type,

-- ORDER ATTRIBUTES (6 columns)
sales_channel,
store_location,
platform,
order_type,
paymentgateway,
paymentmethodcode,

-- CUSTOMER DATA (3 columns)
customer_name,
raw_phone_no_,
customer_identity_status,

-- TIME DIMENSIONS (5 columns)
order_month,
order_week,
order_year,
order_month_num,
year_month,

-- HYPERLOCAL ANALYSIS (5 columns)
hyperlocal_period,
delivery_service_type,
service_tier,
hyperlocal_order_flag,
days_since_hyperlocal_launch,

-- CUSTOMER SEGMENTATION (4 columns)
hyperlocal_customer_segment,
hyperlocal_usage_flag,
hyperlocal_customer_detailed_segment,
hyperlocal_customer_detailed_segment_order,

-- ORDER CLASSIFICATIONS (5 columns)
order_channel,
payment_category,
order_size_category,
customer_tenure_days_at_order,
customer_lifecycle_at_order,

-- RETENTION ANALYTICS - ORDER LEVEL (13 columns)
customer_order_sequence,
channel_order_sequence,
previous_order_date,
previous_channel_order_date,
total_lifetime_orders,
total_channel_orders,
days_since_last_order,
days_since_last_channel_order,
recency_cohort,
customer_engagement_status,
customer_engagement_status_sort,
new_vs_returning,
is_test_customer,

-- ACQUISITION METRICS (7 columns)
acquisition_channel,
acquisition_store,
acquisition_platform,
acquisition_payment_method,
channels_used_count,
stores_visited_count,
channel_preference_type,

-- COHORT ANALYSIS (13 columns)
acquisition_quarter,
acquisition_month,
acquisition_year,
acquisition_quarter_sort,
acquisition_month_sort,
cohort_month_number,
cohort_month_label,
weeks_since_acquisition,
acquisition_week,
is_acquisition_month,
cohort_age_bucket,
acquisition_month_date,
customer_acquisition_date,

transaction_frequency_segment,
transaction_frequency_segment_sort,


cohort_quarter_number,
cohort_quarter_label,
cohort_year_number,
cohort_year_label,


order_value_bucket,
order_value_bucket_sort,

order_channel_detail,

store_location_sort,

-- REPORT METADATA
CURRENT_DATETIME() AS report_last_updated_at, 

-- TOTAL: 65 columns


FROM {{ ref('int_orders') }}