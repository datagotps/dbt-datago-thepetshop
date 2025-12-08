-- CRM Personas: Multi-dimensional segmentation for MoEngage
-- Combines Discount Affinity + RFM + Engagement + Value

SELECT 
    unified_customer_id,
    source_no_,
    customer_name,
    std_phone_no_,
    
    -- Raw segment dimensions
    customer_rfm_segment,
    customer_recency_segment,
    discount_affinity_segment,
    customer_value_segment,
    purchase_frequency_bucket,
    customer_tenure_segment,
    loyalty_enrollment_status,
    
    -- Key metrics for reference
    total_order_count,
    total_sales_value,
    recency_days,
    last_order_date,
    
    -- PERSONA ASSIGNMENT (layered logic)
    CASE 
        -- Priority 1: HIGH-VALUE ACTIVE CUSTOMERS
        WHEN customer_rfm_segment = 'Champions' 
            AND customer_recency_segment IN ('Active', 'Recent')
            AND discount_affinity_segment IN ('High Discount Affinity', 'Medium Discount Affinity')
        THEN 'VIP Discount Lovers'
        
        WHEN customer_rfm_segment = 'Champions' 
            AND customer_recency_segment IN ('Active', 'Recent')
            AND discount_affinity_segment = 'No Discount Usage'
            AND purchase_frequency_bucket IN ('7-10 Orders', '11+ Orders')
        THEN 'Premium Full-Price Buyers'
        
        WHEN customer_rfm_segment IN ('Loyal Customers', 'Potential Loyalists')
            AND customer_recency_segment IN ('Active', 'Recent')
            AND discount_affinity_segment IN ('High Discount Affinity', 'Medium Discount Affinity')
        THEN 'Loyal Deal Seekers'
        
        -- Priority 2: AT-RISK (HIGH VALUE TO RETAIN)
        WHEN customer_rfm_segment = 'Cant Lose Them'
            AND discount_affinity_segment = 'High Discount Affinity'
            AND customer_recency_segment IN ('At Risk', 'Churn')
        THEN 'High-Risk Win-Back'
        
        WHEN customer_rfm_segment = 'At Risk'
            AND customer_recency_segment IN ('At Risk', 'Churn', 'Inactive')
            AND churn_risk_level IN ('High', 'Critical')
        THEN 'At-Risk Reactivation'
        
        -- Priority 3: NEW CUSTOMERS (NURTURE)
        WHEN customer_rfm_segment = 'New Customers'
            AND discount_affinity_segment = 'No Discount Usage'
            AND customer_tenure_segment IN ('1 Month', '3 Months')
            AND purchase_frequency_bucket IN ('1 Order', '2-3 Orders')
        THEN 'New Customer Nurture'
        
        -- Priority 4: LOST/DORMANT (REACTIVATION)
        WHEN customer_rfm_segment = 'Lost'
            AND customer_value_segment IN ('Top 1%', 'Top 20%')
            AND customer_recency_segment = 'Lost'
            AND discount_affinity_segment = 'No Discount Usage'
        THEN 'Lost High-Value'
        
        WHEN purchase_frequency_bucket = '1 Order'
            AND customer_recency_segment IN ('Inactive', 'Lost')
            AND discount_affinity_segment = 'No Discount Usage'
            AND customer_value_segment IN ('Middle 30-60%', 'Bottom 40%')
        THEN 'Lost One-Timer'
        
        -- CATCH-ALL: Other active discount users
        WHEN customer_recency_segment IN ('Active', 'Recent')
            AND discount_affinity_segment IN ('High Discount Affinity', 'Medium Discount Affinity')
        THEN 'Active Discount User'
        
        -- CATCH-ALL: Other active non-discount users
        WHEN customer_recency_segment IN ('Active', 'Recent')
            AND discount_affinity_segment = 'No Discount Usage'
        THEN 'Active Full-Price User'
        
        -- CATCH-ALL: All other inactive/dormant
        WHEN customer_recency_segment IN ('Churn', 'Inactive', 'Lost')
        THEN 'Dormant - General'
        
        ELSE 'Unclassified'
    END AS persona_name,
    
    -- CAMPAIGN ACTION (what to do with each persona)
    CASE 
        WHEN customer_rfm_segment = 'Champions' 
            AND customer_recency_segment IN ('Active', 'Recent')
            AND discount_affinity_segment IN ('High Discount Affinity', 'Medium Discount Affinity')
        THEN 'Regular Discount Campaigns (20-25% off)'
        
        WHEN customer_rfm_segment = 'Champions' 
            AND customer_recency_segment IN ('Active', 'Recent')
            AND discount_affinity_segment = 'No Discount Usage'
            AND purchase_frequency_bucket IN ('7-10 Orders', '11+ Orders')
        THEN 'NO DISCOUNTS - Premium/Loyalty Only'
        
        WHEN customer_rfm_segment IN ('Loyal Customers', 'Potential Loyalists')
            AND customer_recency_segment IN ('Active', 'Recent')
            AND discount_affinity_segment IN ('High Discount Affinity', 'Medium Discount Affinity')
        THEN 'Regular Promotions (15-20% off)'
        
        WHEN customer_rfm_segment = 'Cant Lose Them'
            AND discount_affinity_segment = 'High Discount Affinity'
            AND customer_recency_segment IN ('At Risk', 'Churn')
        THEN 'Aggressive Win-Back (25-30% off)'
        
        WHEN customer_rfm_segment = 'At Risk'
            AND customer_recency_segment IN ('At Risk', 'Churn', 'Inactive')
        THEN 'Reactivation Campaign (20-25% off)'
        
        WHEN customer_rfm_segment = 'New Customers'
            AND discount_affinity_segment = 'No Discount Usage'
            AND customer_tenure_segment IN ('1 Month', '3 Months')
        THEN 'Welcome Series (10-15% off)'
        
        WHEN customer_rfm_segment = 'Lost'
            AND customer_value_segment IN ('Top 1%', 'Top 20%')
        THEN 'Last-Chance Win-Back (25-30% off)'
        
        WHEN purchase_frequency_bucket = '1 Order'
            AND customer_recency_segment IN ('Inactive', 'Lost')
        THEN 'Low Priority Reactivation or Suppress'
        
        WHEN customer_recency_segment IN ('Active', 'Recent')
            AND discount_affinity_segment IN ('High Discount Affinity', 'Medium Discount Affinity')
        THEN 'Standard Discount Campaigns (15-20% off)'
        
        WHEN customer_recency_segment IN ('Active', 'Recent')
            AND discount_affinity_segment = 'No Discount Usage'
        THEN 'Loyalty/Non-Discount Campaigns'
        
        ELSE 'Review Needed'
    END AS recommended_action,
    
    -- SEND DISCOUNT FLAG (simple yes/no)
    CASE 
        WHEN customer_rfm_segment = 'Champions' 
            AND customer_recency_segment IN ('Active', 'Recent')
            AND discount_affinity_segment = 'No Discount Usage'
            AND purchase_frequency_bucket IN ('7-10 Orders', '11+ Orders')
        THEN 'NO'  -- Premium full-price buyers
        
        WHEN customer_recency_segment IN ('Active', 'Recent')
            AND discount_affinity_segment = 'No Discount Usage'
            AND purchase_frequency_bucket IN ('4-6 Orders', '7-10 Orders', '11+ Orders')
        THEN 'NO'  -- Active full-price frequent buyers
        
        ELSE 'YES'  -- Everyone else can receive discounts
    END AS send_discount_flag,
    
    -- PRIORITY LEVEL
    CASE 
        WHEN customer_rfm_segment = 'Champions' 
            AND customer_recency_segment IN ('Active', 'Recent')
        THEN 1  -- Highest priority
        
        WHEN customer_rfm_segment IN ('Loyal Customers', 'Cant Lose Them')
        THEN 2  -- High priority
        
        WHEN customer_rfm_segment = 'At Risk'
        THEN 3  -- Medium priority
        
        WHEN customer_rfm_segment = 'New Customers'
        THEN 4  -- Medium-low priority
        
        WHEN customer_rfm_segment = 'Lost'
        THEN 5  -- Low priority (but high-value lost should be higher)
        
        ELSE 6
    END AS campaign_priority

FROM {{ ref('dim_customers') }}
WHERE customer_acquisition_channel IN ('Online', 'Shop')

ORDER BY campaign_priority, total_sales_value DESC





