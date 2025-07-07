select

  -- Customer Identification
source_no_,  --Customer unique identifier from ERP system
name,  --Customer full name
std_phone_no_,  --Standardized phone number
customer_identity_status,  --Customer identity verification status
loyality_member_id,  --Loyalty program member ID

-- Activity Metrics
active_months_count,  --Number of distinct months customer made purchases
loyalty_enrollment_status,  --'Enrolled' or 'Not Enrolled' in loyalty program

-- Date Metrics
customer_acquisition_date,  --Date of customer's first transaction
first_order_date,  --Same as acquisition date (first transaction)
last_order_date,  --Date of customer's most recent transaction

-- Channel Information
stores_used,  --Pipe-delimited list of physical stores customer has shopped at
platforms_used,  --Pipe-delimited list of online platforms used (website, Android, iOS, etc.)

-- Order Counts
total_order_count,  --Total number of unique orders across all channels
online_order_count,  --Number of orders placed online
offline_order_count,  --Number of orders placed in physical stores

-- Sales Values
total_sales_value,  --Total revenue from customer across all channels
online_sales_value,  --Revenue from online orders
offline_sales_value,  --Revenue from offline/shop orders
ytd_sales,  --Year-to-date sales value
mtd_sales,  --Month-to-date sales value

-- Hyperlocal Metrics (60-min delivery launched 2025-01-16)
pre_hyperlocal_orders,  --Orders placed before hyperlocal launch
pre_hyperlocal_revenue,  --Revenue before hyperlocal launch
post_hyperlocal_orders,  --Orders placed after hyperlocal launch
post_hyperlocal_revenue,  --Revenue after hyperlocal launch
hyperlocal_60min_orders,  --Number of 60-minute express delivery orders
hyperlocal_60min_revenue,  --Revenue from 60-minute express deliveries
express_4hour_orders,  --Number of 4-hour express delivery orders (pre-hyperlocal)
express_4hour_revenue,  --Revenue from 4-hour express deliveries

-- Order Lists
document_ids_list,  --Chronological pipe-delimited list of all order IDs
online_order_ids,  --Chronological pipe-delimited list of online order IDs
offline_order_ids,  --Chronological pipe-delimited list of offline order IDs

-- Acquisition Details
first_acquisition_store,  --Store location for offline-acquired customers
first_acquisition_platform,  --Platform for online-acquired customers (website, Android, iOS)
customer_acquisition_channel,  --'Online' or 'Offline' acquisition channel
first_acquisition_paymentgateway,  --Payment method used for first order
first_acquisition_order_type,  --Order type for first order (EXPRESS, NORMAL, EXCHANGE)
customer_acquisition_channel_detail,  --Specific store or platform where customer was acquired

-- Calculated Metrics
recency_days,  --Days since last order
customer_tenure_days,  --Days since first order
frequency_orders,  --Same as total_order_count
monetary_total_value,  --Same as total_sales_value
monetary_avg_order_value,  --Average order value (total sales / order count)
avg_monthly_demand,  --Average monthly spending (total sales / active months)
months_since_acquisition,  --Number of months since first order

-- Hyperlocal Segmentation
hyperlocal_customer_segment,  --'Acquired Post-Launch' or 'Acquired Pre-Launch'
hyperlocal_usage_flag,  --'Used Hyperlocal' or 'Never Used Hyperlocal'
delivery_service_preference,  --'Both Express Types', '60-Min Only', '4-Hour Only', 'Standard Delivery Only'
hyperlocal_customer_detailed_segment,  --Detailed segment combining acquisition timing and hyperlocal usage
hyperlocal_customer_detailed_segment_order,  --Sort order for detailed hyperlocal segment (1-5)

-- Purchase Behavior
purchase_frequency_bucket,  --'1 Order', '2-3 Orders', '4-6 Orders', '7-10 Orders', '11+ Orders'
purchase_frequency_bucket_order,  --Sort order for frequency buckets (1-6)

-- Recency Segmentation
customer_recency_segment,  --'Active' (0-30d), 'Recent' (31-60d), 'At Risk' (61-90d), 'Churn' (91-180d), 'Inactive' (181-365d), 'Lost' (365+d)
customer_recency_segment_order,  --Sort order for recency segments (1-6)

-- Tenure Segmentation
customer_tenure_segment,  --'1 Month', '3 Months', '6 Months', '1 Year', '2 Years', '3 Years', '4+ Years'
customer_tenure_segment_order,  --Sort order for tenure segments (1-8)

-- Customer Classification
customer_type,  --'New' (1 order in last 30d), 'Repeat' (>1 order), 'One-Time' (1 order >30d ago)
customer_channel_distribution,  --'Hybrid' (both channels), 'Online' only, 'Shop' only
acquisition_cohort,  --Dynamic cohort based on acquisition date (MTD, month-year, or year cohorts)
acquisition_cohort_rank,  --Sort order for acquisition cohorts (higher = more recent)

-- RFM Analysis
r_score,  --Recency score 1-5 (5=most recent)
f_score,  --Frequency score 1-5 based on percentiles (5=most frequent)
m_score,  --Monetary score 1-5 based on percentiles (5=highest value)
rfm_segment,  --3-digit RFM score combination (e.g., '555')
customer_value_segment,  --'Top 1%', 'Top 20%', 'Middle 30-60%', 'Bottom 40%'
customer_value_segment_order,  --Sort order for value segments (1-4)
customer_rfm_segment,  --Named RFM segment: 'Champions', 'Loyal Customers', 'Cant Lose Them', 'Potential Loyalists', 'New Customers', 'At Risk', 'Lost'
customer_rfm_segment_order,  --Sort order for RFM segments (1-7)

-- Order Pattern Analysis
avg_days_between_orders,  --Average days between customer orders
stddev_days_between_orders,  --Standard deviation of days between orders
customer_pattern_type,  --Order consistency: 'No Pattern', 'Perfect Consistency', 'Highly Consistent', 'Consistent', 'Moderately Consistent', 'Variable', 'Highly Variable'
customer_pattern_type_order,  --Sort order for pattern types (0-6)

-- Churn Risk Analysis
churn_risk_score,  --Churn risk score 0-100 (higher = more likely to churn)
churn_risk_level,  --'New Customer', 'Low', 'Medium', 'High', 'Critical'
churn_risk_level_order,  --Sort order for risk levels (0-4)
days_until_expected_order,  --Days until next expected order based on pattern
is_overdue,  --'Yes' or 'No' if customer is overdue for order
overdue_confidence,  --Statistical confidence of being overdue based on std deviation
churn_action_required,  --Recommended action based on churn risk analysis




purchase_frequency_type,  --Customer buying pattern: 'New Customer', 'One-Time Buyer', 'Weekly Buyer', 'Monthly Buyer', 'Quarterly Buyer', 'Annual Buyer', 'Inconsistent Buyer'
purchase_frequency_type_order,  

    -- REPORT METADATA
    CURRENT_DATETIME() AS report_last_updated_at, 


from {{ ref('int_customers') }}
