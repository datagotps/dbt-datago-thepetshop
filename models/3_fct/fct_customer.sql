select

source_no_,
name,
std_phone_no_,
customer_identity_status,
customer_acquisition_date,
first_order_date,
last_order_date,
stores_used,
platforms_used,
total_order_count,
online_order_count,
offline_order_count,
total_sales_value,
online_sales_value,
offline_sales_value,
ytd_sales,
mtd_sales,
first_acquisition_store,
first_acquisition_platform,
customer_acquisition_channel,
recency_days,
customer_tenure_days,
frequency_orders,
monetary_total_value,
monetary_avg_order_value,
avg_monthly_demand,
customer_type,
customer_channel_distribution,
acquisition_cohort,
acquisition_cohort_rank,

r_score,
f_score,
m_score,
rfm_segment,
customer_value_segment, --Top 1%, Top 20, Middle 30-60%, Bottom 40%
customer_value_segment_order,

customer_rfm_segment, -- Champions, Loyal Customers, Potential Loyalists, Cant Lose Them, New Customers, At Risk, Lost
customer_rfm_segment_order,
customer_acquisition_channel_detail,

months_since_acquisition,

document_ids_list,
--online_order_ids,
--offline_order_ids,

first_acquisition_paymentgateway,
first_acquisition_order_type,

purchase_frequency_bucket,
purchase_frequency_bucket_order, --('1 Order', '2-3 Orders', '4-6 Orders', '7-10 Orders', '11+ Orders')

customer_recency_segment,
customer_recency_segment_order,

pre_hyperlocal_orders,
pre_hyperlocal_revenue,
post_hyperlocal_orders,
post_hyperlocal_revenue,
hyperlocal_60min_orders,
hyperlocal_60min_revenue,
express_4hour_orders,
express_4hour_revenue,

hyperlocal_usage_flag, --Used Hyperlocal, Never Used Hyperlocal
delivery_service_preference, --Both Express Types, 60-Min Only, 4-Hour Only, Standard Delivery Only

hyperlocal_customer_detailed_segment, --Post-HL Acq + HL User, Post-HL Acq + Non-HL User, Pre-HL Acq + HL User, Pre-HL Acq + Non-HL User
hyperlocal_customer_detailed_segment_order,

hyperlocal_customer_segment, --Acquired Post-Launch, Acquired Pre-Launch


customer_tenure_segment,
customer_tenure_segment_order,

    -- REPORT METADATA
    CURRENT_DATETIME() AS report_last_updated_at, 


from {{ ref('int_customer_transaction_model') }}
